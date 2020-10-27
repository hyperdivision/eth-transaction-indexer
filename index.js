const Nanoeth = require('nanoeth/http')
const Tail = require('@hyperdivision/eth-transaction-tail')
const Hyperbee = require('hyperbee')
const streamx = require('streamx')

module.exports = class EthIndexer {
  constructor (endpoint, feed, since) {
    this.eth = new Nanoeth(endpoint)
    
    this.feed = feed
    this.db = new Hyperbee(this.feed, {
      valueEncoding: 'json',
      keyEncoding: 'utf-8'
    })
  }

  createTransactionStream (addr) {
    const self = this

    const tr = new streamx.Transform({
      open: () => {
        str.push(self.db.get('!addrs!' + addr))
      }
    })

    const range = {
      gt: '!tx!' + addr + '!',
      lte: '!tx!' + addr + '"'
    }

    return this.db.createReadStream(rang).pipe(tr)
  }

  async add (addr) {
     await this._catchup(90)
     await this._track(addr)
  }

  async start (defaultSeq) {
    const head = await this._head() // see my impl, should return the latest blockNumber
    const since = Math.max(head, defaultSeq)

    const tail = new Tail({
      eth: this.eth,
      since,
      async filter (addr) {
        const node = await this.db.get('!addrs!' + addr)
        return node !== null
      },
      async transaction (tx) {
        if (!(await this.db.get('!addrs!' + tx.to))) return
        return this.db.put(txKey(tx), tx)
      },
      checkpoint (seq) {
        since = seq
      }
    })
  }

  async _head () {
    for await (const { value } of db.createHistoryStream({ reverse: true, limit: 1 })) {
      return value.blockNumber
    }
    return eth.blockNumber()
  }

  async _catchup (minBehind) {
    let tip = Number(await eth.blockNumber())
    while (tip - since > minBehind) {
      await sleep(1000)
      tip = Number(await eth.blockNumber()) 
    } 
  }

  async _track (addr) {
    const id = addr.toLowerCase()

    t.wait(async function () {
      if (await db.get('!addrs!' + id)) return

      const hei = Number(await eth.blockNumber())
      if (hei - since > 100) throw new Error('Tailer is too far behind') // infura only keeps 125 blocks of history on state

      const from = '0x' + Math.max(0, since - 1).toString(16)
      const balance = await eth.getBalance(addr, from)

      await db.put('!addrs!' + id, { date: Date.now(), blockNumber: from, initialBalance: balance })
    })
  }
}

function txKey (tx) {
  return '!tx!' + tx.to + '!' + padBlockNumber(tx.blockNumber) + '!' + padTxNumber(tx.transactionIndex)
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
