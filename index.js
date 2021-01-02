const Nanoeth = require('nanoeth/http')
const Tail = require('@hyperdivision/eth-transaction-tail')
const Hyperbee = require('hyperbee')
const { Readable } = require('streamx')
const thunky = require('thunky/promise')

const BLOCKS_PER_DAY = 8192 // ish
const BALANCE_OF = '0x70a08231'
const MAX_ADDR = '0xffffffffffffffffffffffffffffffffffffffff'
const promiseCallback = (p, cb) => p.then(data => cb(null, data), cb)

module.exports = class EthIndexer {
  constructor (feed, opts = {}) {
    this.since = opts.since || null
    this.live = !!opts.endpoint
    this.confirmations = opts.confirmations
    this.started = null
    this.pollTime = opts.pollTime || 1000

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
    return new TxStream(this.db, addr, { live: true, ...opts })
  }

  async add (addr, opts) {
    if (!this.live) throw new Error('Replicated index cannot access live methods')

    await this.ready()

    if (await this.db.get(addrKey(addr, opts && opts.token))) return

    await this._catchup(90)
    await this._track(addr, opts)
  }

  start () {
    if (this.started) return this.started
    this.started = this._start()
    return this.started
  }

  async _start () {
    if (!this.live) throw new Error('Replicated index cannot access live methods')
    const self = this
    let batch = null
    let lastBlock = null

    await this.ready()

    function storeBlock (block) {
      if (!batch) batch = self.db.batch()
      const key = blockKey(block.number)
      if (lastBlock === key) return
      lastBlock = key
      self.autoCheckpoint = BLOCKS_PER_DAY
      return batch.put(key, blockHeader(block))
    }

    this.tail = new Tail(null, {
      eth: self.eth,
      since: self.since,
      confirmations: self.confirmations,
      pollTime: self.pollTime || 1000,
      async filter (addr) {
        const address = addr ? addr.toLowerCase() : ''
        const node = await self.db.peek({ gte: addrKey(address, null), lt: addrKey(address, MAX_ADDR) })
        return node !== null
      },
      async erc20 (event, tx, _, block, log) {
        const data = {
          type: 'erc20',
          timestamp: block.timestamp,
          token: event.token,
          blockNumber: block.number,
          from: event.from,
          to: event.to,
          value: '0x' + BigInt(event.amount).toString(16)
        }
        const addr = event.to.toLowerCase()

        if (!(await self.db.get(addrKey(addr, event.token)))) return

        if (!batch) batch = self.db.batch()
        await batch.put(erc20Key(addr, event.token, log), data)
        await storeBlock(block)
      },
      async transaction (tx, _, block) {
        const addr = tx.to.toLowerCase()

        if (!(await self.db.get(addrKey(addr, null)))) return

        if (!batch) batch = self.db.batch()
        await batch.put(txKey(tx), { type: 'eth', timestamp: block.timestamp, ...tx })
        await storeBlock(block)
      },
      async block (block) {
        if (--self.autoCheckpoint > 0) return
        await storeBlock(block)
      },
      async checkpoint (seq) {
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
      return Number(value.blockNumber || value.number) + 1
    }
    return this.since || Number(await this.eth.blockNumber())
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

  async _track (addr, opts) {
    if (!this.live) throw new Error('Replicated index cannot access live methods')

    const k = addrKey(addr, opts && opts.token)

    await this.ready()

    return this.tail.wait(async () => {
      if (await this.db.get(k)) return

      const token = (opts && opts.token) || null
      const hei = Number(await this.eth.blockNumber())
      if (hei - this.since > 100) throw new Error('Tailer is too far behind') // infura only keeps 125 blocks of history on state

      const from = '0x' + Math.max(0, this.since - 1).toString(16)
      const balance = token ? await erc20balance(this.eth, addr, token, from) : await this.eth.getBalance(addr, from)
      const startBlock = await this.tail.getBlockByNumber(from)

      const entry = {
        token,
        timestamp: startBlock.timestamp,
        blockNumber: from,
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
    this.addr = addr.toLowerCase()
    this.db = db
    this.live = !!opts.live
    this.token = (opts.token || '').toLowerCase()
  }

  _read (cb) {
    if (this.stream) this.stream.resume()
    cb(null)
  }

  _open (cb) {
    promiseCallback(this.db.get(addrKey(this.addr, this.token)), (err, val) => {
      if (err) return cb(err)
      if (!val) return cb(new Error('Address not tracked'))

      const blockNumber = val.value.blockNumber
      const timestamp = val.value.timestamp

      this.push({
        type: 'balance',
        token: val.value.token,
        blockNumber,
        timestamp,
        value: val.value.initialBalance
      })

      const gt = '!tx!' + this.addr + '!' + this.token + '!'
      const lt = '!tx!' + this.addr + '!' + this.token + '!~'

      let tip = this.db.version ? this.db.version - 1 : 0

      this.stream = this.db.createReadStream({ gt, lt })

      this.stream.on('data', (data) => {
        tip = Math.max(tip, data.seq)
        if (!this.push(data.value)) this.stream.pause()
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
    const prefix = '!tx!' + self.addr + '!' + self.token + '!'

    this.emit('synced')
    this.stream = this.db.createHistoryStream({ gt: start, live: true, limit: -1 })

    this.stream.on('error', (err) => {
      this.destroy(err)
    })

    this.stream.on('data', (data) => {
      if (!filter(data.key)) return
      if (!this.push(data.value)) this.stream.pause()
    })

    function filter (a) {
      return a.slice(0, prefix.length) === prefix
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

async function erc20balance (eth, addr, token, from) {
  return '0x' + BigInt(await eth.call({
    to: token,
    data: BALANCE_OF + addr.slice(2).padStart(64, '0').toLowerCase()
  }, from)).toString(16)
}

function addrKey (addr, token) {
  return '!addr!' + addr.toLowerCase() + '!' + (token || '').toLowerCase()
}

function erc20Key (to, token, log) {
  return '!tx!' + to.toLowerCase() + '!' + token.toLowerCase() + '!' + padBlockNumber(log.blockNumber) + '!' + padTxNumber(log.transactionIndex) + '!' + padTxNumber(log.logIndex)
}

function txKey (tx) {
  return '!tx!' + tx.to.toLowerCase() + '!!' + padBlockNumber(tx.blockNumber) + '!' + padTxNumber(tx.transactionIndex)
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
