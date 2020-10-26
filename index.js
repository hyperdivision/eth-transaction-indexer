const Nanoeth = require('nanoeth/http')
const Tail = require('@hyperdivision/eth-transaction-tail')
const Hyperbee = require('hyperbee')
const hypercore = require('hypercore')

const feed = hypercore('./db')
const db = new Hyperbee(feed, {
  valueEncoding: 'json',
  keyEncoding: 'utf-8'
})

const to = '0x61BAFA4a54F236289F0605Cf4917aD92117A4780'

const eth = new Nanoeth('https://ropsten.infura.io/v3/2aa3f1f44c224eff83b07cef6a5b48b5')

let since = 8952756

const t = new Tail(null, {
  eth,
  since,
  async filter (addr) {
    const node = await db.get('!addrs!' + addr)
    return node !== null
  },
  async transaction (tx) {
    if (!(await db.get('!addrs!' + tx.to))) return
    return db.put(txKey(tx), tx)
  },
  checkpoint (seq) {
    since = seq
    console.log('checkpoint', seq)
  }
})

head().then(console.log)

// track(to)
// t.start()

async function head () {
  for await (const { value } of db.createHistoryStream({ reverse: true, limit: 1 })) {
    return value.blockNumber
  }
  return eth.blockNumber()
}

async function track (addr) {
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

function txKey (tx) {
  return '!tx!' + padBlockNumber(tx.blockNumber) + '!' + padTxNumber(tx.transactionIndex)
}

function padTxNumber (n) {
  return n.slice(2).padStart(8, '0')
}

function padBlockNumber (n) {
  return n.slice(2).padStart(12, '0')
}
