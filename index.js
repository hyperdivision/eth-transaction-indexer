const Nanoeth = require('nanoeth/http')
const Tail = require('@hyperdivision/eth-transaction-tail')
const Hyperbee = require('hyperbee')
const { Readable } = require('streamx')
const UpsertMap = require('upsert-map')

const promiseCallback = (p, cb) => p.then(data => cb(null, data), cb)

module.exports = class EthIndexer {
  constructor (endpoint, feed) {
    this.since = null
    this.eth = new Nanoeth(endpoint)
    this.tail = null
    
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

    const gt = padKey('!tx!' + address + '!')
    const lte = padKey('!tx!' + address + '"')

    const live = new Readable({})
    const tr = new Readable({
      open (cb) {
        promiseCallback(self.db.get('!addrs!' + address), (err, val) => {
          if (err) return cb(err)

          this.push({
            blockNumber: val.value.blockNumber,
            value: val.value.initialBalance
          })
          cb(null)
        })
      },
      destroy () {
        live.destroy()
      }
    })

    const str = this.db.createReadStream({ gt, lte })
      .on('data', data => { tr.push(data.value) })
      .on('end', () => {
        this.streams.upsert(address).add(live)

        tr._read(() => {
          live.resume()
        })

        live.on('data', (data) => {
          if (!tr.push(data)) live.pause()
        })
      })

    return tr
  }

  async add (addr) {
    await this._catchup(90)
    await this._track(addr)
  }

  async start (defaultSeq) {
    const self = this

    const head = await this._head()
    this.since = Math.max(head, defaultSeq)

    this.tail = new Tail(null, {
      eth: self.eth,
      since: self.since,
      async filter (addr) {
        let address = addr ? addr.toLowerCase() : ''
        const node = await self.db.get('!addrs!' + address)
        return node !== null
      },
      async transaction (tx) {
        const addr = tx.to.toLowerCase()

        if (!(await self.db.get('!addrs!' + addr))) return
        self.db.put(txKey(tx), tx)

        if (self.streams.has(addr)) {
          for (let str of self.streams.get(addr)) {
            str.push(tx)
          }
        }
      },
      async checkpoint (seq) {
        self.since = seq
        console.log('checkpoint:', seq)
      }
    })

    this.tail.start()
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

    this.tail.wait(async function () {
      if (await self.db.get('!addrs!' + id)) return

      const hei = Number(await self.eth.blockNumber())
      if (hei - self.since > 100) throw new Error('Tailer is too far behind') // infura only keeps 125 blocks of history on state

      const from = '0x' + Math.max(0, self.since - 1).toString(16)
      const balance = await self.eth.getBalance(addr, from)

      await self.db.put('!addrs!' + id, { date: Date.now(), blockNumber: from, initialBalance: balance })
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

function padKey (key) {
  return key.padEnd(68, '\u0000')
}

function sleep (n) {
  return new Promise(resolve => setTimeout(resolve, n))
}
