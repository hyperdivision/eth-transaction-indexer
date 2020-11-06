const Indexer = require('./')
const hypercore = require('hypercore')

const feed = hypercore('./db')

const to = '0x50c7d91e74b0e42bd8bce8ad6d199e4a23c0b193'
const url = 'https://ropsten.infura.io/v3/2aa3f1f44c224eff83b07cef6a5b48b5'
const since = 0x899e43 - 200

const index = new Indexer(url, feed, since)

index.start()
index.add(to).then(() => {
  const str = index.createTransactionStream(to)
  str.on('data', d => console.log('-- data --', d))
})
