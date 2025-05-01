package network

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/guilhermelinosp/litecoin-go/blockchain"
	"github.com/guilhermelinosp/litecoin-go/proto/node"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/sirupsen/logrus"
)

const (
	ProtocolID      protocol.ID = "/litecoin-go/1.0.0"
	MaxWorkers      int         = 10
	DefaultGRPCPort int         = 50051
	MaxMessageSize  int         = 1024 * 1024
)

type Node struct {
	Host      host.Host
	ctx       context.Context
	peers     map[peer.ID]*peer.AddrInfo
	peersLock sync.RWMutex
	logger    *logrus.Logger
}

func NewNode(ctx context.Context) (*Node, error) {
	// Generate a new key pair
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	// Create a new libp2p host
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.DefaultListenAddrs,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create host: %w", err)
	}

	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	node := &Node{
		Host:   h,
		ctx:    ctx,
		peers:  make(map[peer.ID]*peer.AddrInfo),
		logger: logger,
	}

	h.SetStreamHandler(ProtocolID, node.handleStream)
	node.monitorPeers()
	return node, nil
}

func (n *Node) Connect(ctx context.Context, peerInfo *peer.AddrInfo) error {
	if err := n.Host.Connect(ctx, *peerInfo); err != nil {
		return fmt.Errorf("failed to connect to peer %s: %v", peerInfo.ID, err)
	}
	n.peersLock.Lock()
	defer n.peersLock.Unlock()
	n.peers[peerInfo.ID] = peerInfo
	n.logger.Infof("Connected to peer: %s", peerInfo.ID)
	return nil
}

func (n *Node) handleStream(stream network.Stream) {
	defer stream.Close()

	for {
		select {
		case <-n.ctx.Done():
			n.logger.Info("Stream handler shutting down due to context cancellation")
			return
		default:
			// Read length-prefixed message
			lenBuf := make([]byte, 4)
			if _, err := io.ReadFull(stream, lenBuf); err != nil {
				if err != io.EOF {
					n.logger.Errorf("Error reading message length: %v", err)
				}
				return
			}
			length := binary.BigEndian.Uint32(lenBuf)
			if length > uint32(MaxMessageSize) {
				n.logger.Errorf("Message too large: %d bytes", length)
				return
			}
			buf := make([]byte, length)
			if _, err := io.ReadFull(stream, buf); err != nil {
				n.logger.Errorf("Error reading message: %v", err)
				return
			}
			n.logger.Infof("Received message from %s: %s", stream.Conn().RemotePeer(), string(buf))

			// Write response
			if _, err := stream.Write([]byte("Acknowledged")); err != nil {
				n.logger.Errorf("Error writing response: %v", err)
			}
		}
	}
}

func (n *Node) AddrInfo() *peer.AddrInfo {
	return &peer.AddrInfo{
		ID:    n.Host.ID(),
		Addrs: n.Host.Addrs(),
	}
}

func (n *Node) BroadcastBlock(block *blockchain.Block) error {
	protoBlock := &node.Block{
		Timestamp:    block.Timestamp,
		PrevHash:     block.PrevHash,
		Hash:         block.Hash,
		Transactions: n.convertTransactionsToProto(block.Transactions),
		Nonce:        block.Nonce,
		Difficulty:   block.Difficulty,
	}

	n.peersLock.RLock()
	peers := make([]*peer.AddrInfo, 0, len(n.peers))
	for _, pi := range n.peers {
		peers = append(peers, pi)
	}
	n.peersLock.RUnlock()

	workerCh := make(chan *peer.AddrInfo, len(peers))
	errCh := make(chan error, len(peers))
	var wg sync.WaitGroup

	// Start worker pool
	for i := 0; i < MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pi := range workerCh {
				select {
				case <-n.ctx.Done():
					errCh <- n.ctx.Err()
					return
				default:
					addr := fmt.Sprintf("%s:%d", pi.Addrs[0].String(), DefaultGRPCPort)
					client, err := NewGRPCClient(addr, n)
					if err != nil {
						errCh <- fmt.Errorf("failed to create gRPC client for %s: %v", pi.ID, err)
						continue
					}
					defer client.Close()

					ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
					defer cancel()

					if _, err := client.client.SendBlock(ctx, protoBlock); err != nil {
						errCh <- fmt.Errorf("failed to send block to %s: %v", pi.ID, err)
					} else {
						n.logger.Infof("Successfully sent block to %s", pi.ID)
					}
				}
			}
		}()
	}

	// Send peers to workers
	for _, pi := range peers {
		workerCh <- pi
	}
	close(workerCh)
	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("broadcast errors: %v", errs)
	}
	return nil
}

func (n *Node) monitorPeers() {
	n.Host.Network().Notify(&network.NotifyBundle{
		DisconnectedF: func(_ network.Network, conn network.Conn) {
			n.peersLock.Lock()
			peerID := conn.RemotePeer()
			delete(n.peers, peerID)
			n.peersLock.Unlock()
			n.logger.Infof("Peer disconnected: %s", peerID)
		},
	})
}

func (n *Node) convertTransactionsToProto(transactions []*blockchain.Transaction) []*node.Transaction {
	protoTransactions := make([]*node.Transaction, len(transactions))
	for i, tx := range transactions {
		protoTransactions[i] = &node.Transaction{
			Id:      tx.ID,
			Inputs:  n.convertInputsToProto(tx.Inputs),
			Outputs: n.convertOutputsToProto(tx.Outputs),
		}
	}
	return protoTransactions
}

func (n *Node) convertInputsToProto(inputs []blockchain.TxInput) []*node.TxInput {
	protoInputs := make([]*node.TxInput, len(inputs))
	for i, input := range inputs {
		protoInputs[i] = &node.TxInput{
			TxId:      input.TxID,
			OutIdx:    input.OutIdx,
			Signature: input.Signature,
			PubKey:    input.PubKey,
		}
	}
	return protoInputs
}

func (n *Node) convertOutputsToProto(outputs []blockchain.TxOutput) []*node.TxOutput {
	protoOutputs := make([]*node.TxOutput, len(outputs))
	for i, output := range outputs {
		protoOutputs[i] = &node.TxOutput{
			Value:      output.Value,
			PubKeyHash: output.PubKeyHash,
		}
	}
	return protoOutputs
}

func (n *Node) convertTransactionsToBlockchain(transactions []*node.Transaction) []*blockchain.Transaction {
	converted := make([]*blockchain.Transaction, len(transactions))
	for i, t := range transactions {
		converted[i] = &blockchain.Transaction{
			ID:      t.Id,
			Inputs:  n.convertInputsToBlockchain(t.Inputs),
			Outputs: n.convertOutputsToBlockchain(t.Outputs),
		}
	}
	return converted
}

func (n *Node) convertInputsToBlockchain(inputs []*node.TxInput) []blockchain.TxInput {
	converted := make([]blockchain.TxInput, len(inputs))
	for i, input := range inputs {
		converted[i] = blockchain.TxInput{
			TxID:      input.TxId,
			OutIdx:    input.OutIdx,
			Signature: input.Signature,
			PubKey:    input.PubKey,
		}
	}
	return converted
}

func (n *Node) convertOutputsToBlockchain(outputs []*node.TxOutput) []blockchain.TxOutput {
	converted := make([]blockchain.TxOutput, len(outputs))
	for i, output := range outputs {
		converted[i] = blockchain.TxOutput{
			Value:      output.Value,
			PubKeyHash: output.PubKeyHash,
		}
	}
	return converted
}

func (n *Node) convertTransactionsFromProto(protoTransactions []*node.Transaction) []*blockchain.Transaction {
	transactions := make([]*blockchain.Transaction, len(protoTransactions))
	for i, protoTx := range protoTransactions {
		transactions[i] = &blockchain.Transaction{
			ID:      protoTx.Id,
			Inputs:  n.convertInputsToBlockchain(protoTx.Inputs),
			Outputs: n.convertOutputsToBlockchain(protoTx.Outputs),
		}
	}
	return transactions
}
