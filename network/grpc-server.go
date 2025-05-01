package network

import (
	"context"
	"log"
	"net"

	"github.com/guilhermelinosp/litecoin-go/blockchain"
	"github.com/guilhermelinosp/litecoin-go/proto/node"
	"google.golang.org/grpc"
)

type GRPCServer struct {
	node.UnimplementedNodeServiceServer
	blockchain *blockchain.Blockchain
	mempool    *blockchain.Mempool
	node       *Node
}

func (s *GRPCServer) GetBlockchain(ctx context.Context, req *node.Empty) (*node.BlockchainResponse, error) {
	blocks := s.blockchain.GetBlocks()
	protoBlocks := make([]*node.Block, 0, len(blocks))

	for _, b := range blocks {
		protoBlocks = append(protoBlocks, &node.Block{
			Timestamp:    b.Timestamp,
			PrevHash:     b.PrevHash,
			Hash:         b.Hash,
			Transactions: s.node.convertTransactionsToProto(b.Transactions),
			Nonce:        b.Nonce,
			Difficulty:   b.Difficulty,
		})
	}

	return &node.BlockchainResponse{
		Blocks:        protoBlocks,
		CurrentHeight: int64(len(blocks)),
	}, nil
}

func (s *GRPCServer) SendBlock(ctx context.Context, block *node.Block) (*node.Ack, error) {
	newBlock := &blockchain.Block{
		Timestamp:    block.Timestamp,
		PrevHash:     block.PrevHash,
		Hash:         block.Hash,
		Transactions: s.node.convertTransactionsFromProto(block.Transactions),
		Nonce:        block.Nonce,
		Difficulty:   block.Difficulty,
	}

	if err := s.blockchain.AddBlock(newBlock); err != nil {
		return &node.Ack{Success: false, Message: err.Error()}, nil
	}

	// Broadcast to other peers
	go s.node.BroadcastBlock(newBlock)

	return &node.Ack{Success: true, Message: "Block accepted"}, nil
}

func StartGRPCServer(bc *blockchain.Blockchain, mp *blockchain.Mempool, n *Node, port string) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	node.RegisterNodeServiceServer(s, &GRPCServer{
		blockchain: bc,
		mempool:    mp,
		node:       n,
	})

	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
