package network

import (
	"context"
	"fmt"
	"time"

	"github.com/guilhermelinosp/litecoin-go/blockchain"
	"github.com/guilhermelinosp/litecoin-go/proto/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCClient struct {
	conn   *grpc.ClientConn
	client node.NodeServiceClient
	node   *Node
}

func NewGRPCClient(addr string, n *Node) (*GRPCClient, error) {
	// Define gRPC dial options
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server at %s: %w", addr, err)
	}

	return &GRPCClient{
		conn:   conn,
		client: node.NewNodeServiceClient(conn),
		node:   n,
	}, nil
}

func (c *GRPCClient) SyncBlockchain() ([]*blockchain.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.client.GetBlockchain(ctx, &node.Empty{})
	if err != nil {
		return nil, err
	}

	blocks := make([]*blockchain.Block, 0, len(resp.Blocks))
	for _, b := range resp.Blocks {
		blocks = append(blocks, &blockchain.Block{
			Timestamp:    b.Timestamp,
			PrevHash:     b.PrevHash,
			Hash:         b.Hash,
			Transactions: c.node.convertTransactionsToBlockchain(b.Transactions), // Use the new conversion function
			Nonce:        b.Nonce,
			Difficulty:   b.Difficulty,
		})
	}

	return blocks, nil
}

func (c *GRPCClient) Close() error {
	return c.conn.Close()
}
