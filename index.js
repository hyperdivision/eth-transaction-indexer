const Nanoeth = require('nanoeth/http')
const Tail = require('@hyperdivision/eth-transaction-tail')
const Hyperbee = require('hyperbee')
const { Readable } = require('streamx')
const thunky = require('thunky/promise')
const UpsertMap = require('upsert-map')

const promiseCallback = (p, cb) => p.then(data => cb(null, data), cb)

module.exports = class EthIndexer {
  constructor (endpoint, feed, defaultSeq) {
    this.since = null
    this.eth = new Nanoeth(endpoint)
    this.tail = null

    this.ready = thunky(async () => {
      const head = await this._head()
      this.since = Math.max(head, defaultSeq)
    })

    this.feed = feed
    this.db = new Hyperbee(this.feed, {
      valueEncoding: 'json',
      keyEncoding: 'utf-8'
    })

    this.streams = new UpsertMap(() => new Set(), set => !set.size)
  }

  createTransactionStream (addr) {
    const self = this

    const address = addr.toLowerCase()

    const txns = new TxStream(this.db, address, { live: true })
    this.streams.upsert(addr).add(txns)

    txns.on('close', () => this.streams.get(addr).delete(txns))
    return txns
  }

  async add (addr) {
    await this.ready()
    await this._catchup(90)
    await this._track(addr)
  }

  async start () {
    const self = this

    await this.ready()

    this.tail = new Tail(null, {
      eth: self.eth,
      since: self.since,
      async filter (addr) {
        const address = addr ? addr.toLowerCase() : ''
        const node = await self.db.get('!addrs!' + address)
        return node !== null
      },
      async transaction (tx) {
        const addr = tx.to.toLowerCase()

        if (!(await self.db.get('!addrs!' + addr))) return
        await self.db.put(txKey(tx), tx)

        if (self.streams.has(addr)) {
          for (const str of self.streams.get(addr)) {
            str.pushLive(tx)
          }
        }
      },
      async checkpoint (seq) {
        self.since = seq
      }
    })

    return this.tail.start()
  }

  async _head () {
    for await (const { value } of this.db.createHistoryStream({ reverse: true, limit: 1 })) {
      return value.blockNumber
    }
    return this.eth.blockNumber()
  }

  async _catchup (minBehind) {
    if (this.since === null) throw new Error('Tailer has not started')

    let tip = Number(await this.eth.blockNumber())
    while (tip - this.since > minBehind) {
      await sleep(1000)
      tip = Number(await this.eth.blockNumber())
    }
  }

  async _track (addr) {
    const self = this
    const id = addr.toLowerCase()

    await this.ready()

    this.tail.wait(async function () {
      if (await self.db.get('!addrs!' + id)) return

      const hei = Number(await self.eth.blockNumber())
      if (hei - self.since > 100) throw new Error('Tailer is too far behind') // infura only keeps 125 blocks of history on state

      const from = '0x' + Math.max(0, self.since - 1).toString(16)
      const balance = await self.eth.getBalance(addr, from)

      await self.db.put('!addrs!' + id, { date: Date.now(), blockNumber: from, initialBalance: balance })
    })
  }

  async stop () {
    await this.ready()
    await this.tail.stop(true)

    for (const [addr, streams] of this.streams) {
      for (const stream of streams) {
        await stream.destroy()
        this.streams.get(addr).delete(stream)
      }
    }
  }
}

class TxStream extends Readable {
  constructor (db, addr, opts) {
    super()

    this.live = opts.live
    this.addr = addr
    this.pending = []
    this.db = db
    this.live = !!opts.live
  }

  pushLive (data) {
    if (this.pending) this.pending.push(data)
    else this.push(data)
  }

  _read (cb) {
    if (this.stream) this.stream.resume()
    cb(null)
  }

  _open (cb) {
    const self = this
    promiseCallback(this.db.get('!addrs!' + this.addr), (err, val) => {
      if (err) return cb(err)

      self.push({
        blockNumber: val.value.blockNumber,
        value: val.value.initialBalance
      })

      const gt = '!tx!' + self.addrs + '!'
      const lt = '!tx!' + self.addrs + '"'

      self.stream = self.db.createReadStream({ gt, lt })

      let lastKey = ''
      self.stream.on('data', (data) => {
        lastKey = data.key
        if (!self.push(data)) self.stream.pause()
      })

      self.stream.on('end', () => {
        self.stream = null

        if (!self.live) {
          self.push(null) // end it now
          return
        }

        while (self.pending.length) {
          const next = self.pending.shift()
          if (lastKey < txKey(next)) continue // we already emitted this
          self.push(next)
        }
        self.pending = null
      })

      cb(null)
    })
  }
}

function txKey (tx) {
  return '!tx!' + tx.to.toLowerCase() + '!' + padBlockNumber(tx.blockNumber) + '!' + padTxNumber(tx.transactionIndex)
}

function padTxNumber (n) {
  return n.slice(2).padStart(8, '0')
}

function padBlockNumber (n) {
  return n.slice(2).padStart(12, '0')
}

function sleep (n) {
  return new Promise(resolve => setTimeout(resolve, n))
}
