# eth-transaction-indexer

Tails transactions on the Ethereum blockchain and indexes them in a Hyperbee

```
npm install @hyperdivision/eth-transaction-indexer
```

## Usage

``` js
const Indexer = require('eth-transaction-indexer')

const testnet = 'https://ropsten.infura.io/v3/2aa3f1f44c224eff83b07cef6a5b48b5'
const feed = hypercore('./path/to/storage')

// start indexing from block number
const beginAt = 0x899b00

const index = new Indexer(feed, {
  endpoint: testnet
})

// start the indexer
index.start()

// add an address to the index
index.add('0xdea..dbeef')

// get a stream of all the transactions paying to an address
const txns = index.createTransactionStream('0xdea..dbeef')

for await (let tx of txns) console.log(tx)
```

## API

#### `const index = new Indexer(feed, opts)`

Instantiate a new index. Transactions of interest are logged in a Hyperbee written to the hypercore passed in as `feed`. `endpoint` should be an Ethereum HTTP API. `defaultSeq` gives the block number from which to start indexing from.

```js
const opts = {
  endpoint: 'http://...',  // chain backend API
}
```

#### `index.start([defaultSeq])`

Start tailing the blockchain.

#### `index.add(addr)`

Begin tracking transactions paying to `addr`. The initial balance of the address will be stored under the key `!addrs!<addr>`, subsequent transactions will be added under the key `!tx!<addr>!<block_number>!<tx_number>`.

#### `const str = index.createTransactionStream(addr)`

Retrieve all transactions paying to the address given by `addr`. Returns an live stream of the time ordered transactions with the address' initial balance inserted before the first transaction.

The initial balance is inserted as a tx of the form:
```
{
  value: 0x4572e2,      // initial balance of the address
  blockNumber: 0x899b00 // block number at which the indexer began tracking the address
}
```

Other transactions are full eth transaction objects:
```
{
  blockHash: '0x277dfee78f6b1d8e495e7c25c95e0f2dc36a4ca772b32aa7b1ae7107f21d1351',
  blockNumber: '0x899f1b',
  from: '0x301b695ae7ba03b63828b6668232063bd4c9e0bc',
  gas: '0x5208',
  gasPrice: '0x7aef40a00',
  hash: '0x7cde8e856bf1c73146c64054f0e74acdd05d7ba351693c6aaeca620bcdcfc03e',
  input: '0x',
  nonce: '0x16',
  r: '0xf38ed059c881c3079d445081c09e6f0df7cfa5fa9be1cb830b479fbb1a0327c8',
  s: '0x63830c84ef2b177d9ec488eeb32215363364aa7aade743c403d2a06686b7fc95',
  to: '0x50c7d91e74b0e42bd8bce8ad6d199e4a23c0b193',
  transactionIndex: '0x0',
  v: '0x2a',
  value: '0x5af3107a4000'
}
```
