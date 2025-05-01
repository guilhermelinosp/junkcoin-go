package blockchain

import (
	"encoding/json"
	"sync"
)

type Transaction struct {
	ID      []byte
	Inputs  []TxInput
	Outputs []TxOutput
}

func (tx *Transaction) Serialize() []byte {
	data, _ := json.Marshal(tx)
	return data
}

type TxInput struct {
	TxID      []byte
	OutIdx    int64
	Signature []byte
	PubKey    []byte
}

type TxOutput struct {
	Value      int64
	PubKeyHash []byte
}

type Mempool struct {
	transactions map[string]*Transaction
	mu           sync.RWMutex
}

func NewMempool() *Mempool {
	return &Mempool{
		transactions: make(map[string]*Transaction),
	}
}

func (mp *Mempool) AddTransaction(tx *Transaction) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.transactions[string(tx.ID)] = tx
}

func (mp *Mempool) GetTransactions() []*Transaction {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	txs := make([]*Transaction, 0, len(mp.transactions))
	for _, tx := range mp.transactions {
		txs = append(txs, tx)
	}
	return txs
}

func (mp *Mempool) RemoveTransactions(txs []*Transaction) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	for _, tx := range txs {
		delete(mp.transactions, string(tx.ID))
	}
}

