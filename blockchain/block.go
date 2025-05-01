package blockchain

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"sync"
	"time"
)

type Block struct {
	Timestamp    int64
	Transactions []*Transaction
	PrevHash     []byte
	Hash         []byte
	Nonce        int64
	Difficulty   int64
}

type Blockchain struct {
	blocks     []*Block
	difficulty int64
	mu         sync.Mutex
}

func NewBlockchain(difficulty int64) *Blockchain {
	genesisBlock := &Block{
		Timestamp:    time.Now().Unix(),
		Transactions: []*Transaction{},
		PrevHash:     []byte{},
		Nonce:        0,
		Difficulty:   difficulty,
	}
	genesisBlock.Hash = genesisBlock.CalculateHash()
	return &Blockchain{
		blocks:     []*Block{genesisBlock},
		difficulty: difficulty,
	}
}

func (bc *Blockchain) AddBlock(block *Block) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	lastBlock := bc.blocks[len(bc.blocks)-1]
	if !bytes.Equal(block.PrevHash, lastBlock.Hash) {
		return errors.New("invalid previous hash")
	}

	if !block.ValidatePoW(bc.difficulty) {
		return errors.New("invalid proof of work")
	}

	bc.blocks = append(bc.blocks, block)
	return nil
}

func (b *Block) ValidatePoW(difficulty int64) bool {
	target := bytes.Repeat([]byte{0}, int(difficulty))
	hash := b.CalculateHash()
	return bytes.Compare(hash[:difficulty], target) == 0
}

func (b *Block) SerializeTransactions() []byte {
	var serialized []byte
	for _, tx := range b.Transactions {
		serialized = append(serialized, tx.Serialize()...)
	}
	return serialized
}

func (b *Block) CalculateHash() []byte {
	data := bytes.Join(
		[][]byte{
			b.PrevHash,
			b.SerializeTransactions(),
			Int64ToBytes(b.Timestamp),
			Int64ToBytes(b.Nonce),
			Int64ToBytes(b.Difficulty),
		},
		[]byte{},
	)
	hash := sha256.Sum256(data)
	return hash[:]
}

func (bc *Blockchain) GetBlocks() []*Block {
	return bc.blocks
}

func (bc *Blockchain) GetDifficulty() int64 {
	return bc.difficulty
}

func Int64ToBytes(num int64) []byte {
	buf := make([]byte, 8)
	for i := range 8 {
		buf[i] = byte(num >> uint(8*(7-i)))
	}
	return buf
}
