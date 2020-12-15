const Indexer = require('./')
const hypercore = require('hypercore')

const feed = hypercore('./db')

const to = '0x50c7d91e74b0e42bd8bce8ad6d199e4a23c0b193'
const url = 'https://ropsten.infura.io/v3/2aa3f1f44c224eff83b07cef6a5b48b5'
const since = 0 // 9259801 - 20
const index = new Indexer(feed, { endpoint: url, since, confirmations: 0 })

index.start()

/*
index.add(to).then(() => {
  const str = index.createTransactionStream(to)
  str.on('data', d => { console.log('1: -- data --', d) })
})
*/

const token = '0x101848d5c5bbca18e6b4431eedf6b95e9adf82fa'
index.add('0x61bafa4a54f236289f0605cf4917ad92117a4780', { token }).then(function () {
  const str = index.createTransactionStream('0x61bafa4a54f236289f0605cf4917ad92117a4780', { live: true, token })
  str.on('data', d => { console.log('2: -- data --', d) })
})
