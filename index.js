const Nanoeth = require('nanoeth/http')
const Tail = require('@hyperdivision/eth-transaction-tail')
const Hyperbee = require('hyperbee')
const { Readable } = require('streamx')
const thunky = require('thunky/promise')

const BLOCKS_PER_DAY = 8192 // ish
const promiseCallback = (p, cb) => p.then(data => cb(null, data), cb)

module.exports = class EthIndexer {
  constructor (feed, opts = {}) {
    this.since = null
    this.live = !!opts.endpoint

    this.tail = null
    this.eth = this.live ? new Nanoeth(opts.endpoint) : null

    this.ready = thunky(async () => {
      if (!this.live) return
      const seq = opts.defaultSeq || 0
      this.since = Math.max(await this._head(), seq)
    })

    this.autoCheckpoint = BLOCKS_PER_DAY
    this.feed = feed
    this.db = new Hyperbee(this.feed, {
      valueEncoding: 'json',
      keyEncoding: 'utf-8'
    })
  }

  createTransactionStream (addr, opts) {
    const address = addr.toLowerCase()

    return new TxStream(this.db, address, { live: true, ...opts })
  }

  async add (addr) {
    if (!this.live) throw new Error('Replicated index cannot access live methods')

    await this.ready()

    if (await this.db.get(addrKey(addr))) return

    await this._catchup(90)
    await this._track(addr)
  }

  async start () {
    if (!this.live) throw new Error('Replicated index cannot access live methods')
    const self = this
    let batch = null
    let lastBlock = null

    await this.ready()

    this.tail = new Tail(null, {
      eth: self.eth,
      since: self.since,
      async filter (addr) {
        const address = addr ? addr.toLowerCase() : ''
        const node = await self.db.get(addrKey(address))
        return node !== null
      },
      async transaction (tx, _, block) {
        const addr = tx.to.toLowerCase()

        if (!(await self.db.get(addrKey(addr)))) return

        if (!batch) batch = self.db.batch()
        await batch.put(txKey(tx), tx)

        const key = blockKey(block.number)
        if (lastBlock === key) return

        lastBlock = key
        self.autoCheckpoint = BLOCKS_PER_DAY
        await batch.put(key, blockHeader(block))
      },
      async block (block) {
        if (--self.autoCheckpoint > 0) return

        if (!batch) batch = self.db.batch()
        const key = blockKey(block.number)
        lastBlock = key
        self.autoCheckpoint = BLOCKS_PER_DAY
        await batch.put(key, blockHeader(block))
      },
      async checkpoint (seq) {
        console.log(self.autoCheckpoint, seq)
        if (batch) {
          await batch.flush()
          batch = null
        }
        self.since = seq
      }
    })

    return this.tail.start()
  }

  async _head () {
    if (!this.live) throw new Error('Replicated index cannot access live methods')
    for await (const { value } of this.db.createHistoryStream({ reverse: true, limit: 1 })) {
      return Number(value.blockNumber || value.number)
    }
    return Number(await this.eth.blockNumber())
  }

  async _catchup (minBehind) {
    if (!this.live) throw new Error('Replicated index cannot access live methods')
    if (this.since === null) throw new Error('Tailer has not started')

    let tip = Number(await this.eth.blockNumber())
    while (tip - this.since > minBehind) {
      await sleep(1000)
      tip = Number(await this.eth.blockNumber())
    }
  }

  async _track (addr) {
    if (!this.live) throw new Error('Replicated index cannot access live methods')

    const k = addrKey(addr)

    await this.ready()

    return this.tail.wait(async () => {
      if (await this.db.get(k)) return

      const hei = Number(await this.eth.blockNumber())
      if (hei - this.since > 100) throw new Error('Tailer is too far behind') // infura only keeps 125 blocks of history on state

      const from = '0x' + Math.max(0, this.since - 1).toString(16)
      const balance = await this.eth.getBalance(addr, from)
      const startBlock = await this.tail.getBlockByNumber(from)

      const entry = {
        blockNumber: from,
        timestamp: startBlock.timestamp,
        initialBalance: balance
      }

      const batch = this.db.batch()

      await batch.put(k, entry)
      await batch.flush()
    })
  }

  async stop () {
    if (!this.live) throw new Error('Replicated index cannot access live methods')

    await this.ready()
    await this.tail.stop(true)
    return new Promise((resolve, reject) => {
      this.feed.close(err => {
        if (err) reject(err)
        else resolve()
      })
    })
  }
}

class TxStream extends Readable {
  constructor (db, addr, opts) {
    super()

    this.live = opts.live
    this.addr = addr
    this.db = db
    this.live = !!opts.live
  }

  _read (cb) {
    if (this.stream) this.stream.resume()
    cb(null)
  }

  _open (cb) {
    promiseCallback(this.db.get(addrKey(this.addr)), (err, val) => {
      if (err) return cb(err)

      const blockNumber = val.value.blockNumber
      const timestamp = val.value.timestamp

      this.push({
        blockNumber,
        timestamp,
        value: val.value.initialBalance
      })

      const gt = '!tx!' + this.addr + '!'
      const lt = '!tx!' + this.addr + '~'

      let tip = this.db.version || 0

      this.stream = this.db.createReadStream({ gt, lt })

      this.stream.on('data', (data) => {
        tip = Math.max(tip, data.seq)
        if (!this.push(data)) this.stream.pause()
      })

      this.stream.on('error', (err) => {
        this.destroy(err)
      })

      this.stream.on('end', () => {
        if (!this.live) {
          this.push(null) // end it now
          this.emit('synced')
          return
        }

        this._liveStream(tip)
      })

      cb(null)
    })
  }

  _liveStream (start) {
    const self = this

    this.emit('synced')

    this.stream = this.db.createHistoryStream({ gt: start, live: true, limit: -1 })

    this.stream.on('error', (err) => {
      this.destroy(err)
    })

    this.stream.on('data', (data) => {
      if (!filter(data.key)) return
      if (!this.push(data)) this.stream.pause()
    })

    function filter (a) {
      return a.slice(0, 46) === '!tx!' + self.addr.toLowerCase()
    }
  }

  _predestroy () {
    if (this.stream) this.stream.destroy()
  }

  _destroy (cb) {
    if (this.stream) this.stream.destroy()
    cb()
  }
}

function addrKey (addr) {
  return '!addr!' + addr.toLowerCase()
}

function txKey (tx) {
  return '!tx!' + tx.to.toLowerCase() + '!' + padBlockNumber(tx.blockNumber) + '!' + padTxNumber(tx.transactionIndex)
}

function blockKey (seq) {
  return '!block!' + padBlockNumber(seq)
}

function padTxNumber (n) {
  return n.slice(2).padStart(8, '0')
}

function padBlockNumber (n) {
  return n.slice(2).padStart(12, '0')
}

function blockHeader (block) {
  const obj = {}
  for (const key of Object.keys(block)) {
    if (key !== 'transactions') obj[key] = block[key]
  }
  return obj
}

function sleep (n) {
  return new Promise(resolve => setTimeout(resolve, n))
}
