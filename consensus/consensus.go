package consensus

import (
	"time"

	"github.com/guilhermelinosp/litecoin-go/blockchain"
	"github.com/guilhermelinosp/litecoin-go/network"
)

type Consensus struct {
	blockchain *blockchain.Blockchain
	mempool    *blockchain.Mempool
	node       *network.Node
	mining     bool
	miningStop chan struct{}
}

func NewConsensus(bc *blockchain.Blockchain, mp *blockchain.Mempool, node *network.Node) *Consensus {
	return &Consensus{
		blockchain: bc,
		mempool:    mp,
		node:       node,
		miningStop: make(chan struct{}),
	}
}

func (c *Consensus) StartMining() {
	c.mining = true
	go c.mineBlocks()
}

func (c *Consensus) StopMining() {
	if c.mining {
		c.mining = false
		close(c.miningStop)
	}
}

func (c *Consensus) mineBlocks() {
	for c.mining {
		select {
		case <-c.miningStop:
			return
		default:
			txs := c.mempool.GetTransactions()
			if len(txs) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			lastBlock := c.blockchain.GetBlocks()[len(c.blockchain.GetBlocks())-1]
			newBlock := &blockchain.Block{
				Timestamp:    time.Now().Unix(),
				Transactions: txs,
				PrevHash:     lastBlock.Hash,
				Difficulty:   c.blockchain.GetDifficulty(),
				Nonce:        0,
			}

			for c.mining {
				newBlock.Nonce++
				newBlock.Hash = newBlock.CalculateHash()
				if newBlock.ValidatePoW(c.blockchain.GetDifficulty()) {
					if err := c.blockchain.AddBlock(newBlock); err == nil {
						c.mempool.RemoveTransactions(txs)
						c.node.BroadcastBlock(newBlock)
					}
					break
				}
			}
		}
	}
}
