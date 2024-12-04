package controller

import (
	"adaptodb/pkg/schema"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	pb "adaptodb/pkg/proto/proto"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Operator struct {
	fromShard schema.RaftGroup
	toShard   schema.RaftGroup
	fromNode  schema.NodeInfo
	toNode    schema.NodeInfo
	keyRanges []schema.KeyRange
	cancel    context.CancelFunc
	done      chan struct{}
	taskId    uint64
}

func NewOperator(fromShard schema.RaftGroup, toShard schema.RaftGroup, keyRanges []schema.KeyRange, cancel context.CancelFunc) *Operator {
	taskId := rand.Uint64()
	return &Operator{
		taskId:    taskId,
		fromShard: fromShard,
		toShard:   toShard,
		keyRanges: keyRanges,
		cancel:    cancel,
	}
}

func getRandomNodeHTTPServer(shard schema.RaftGroup) (schema.NodeInfo, string) {
	// idx := uint64(rand.Intn(len(shard.Members)))
	idx := uint64(0)
	node := shard.Members[idx]
	httpAddress := fmt.Sprintf("%s:%d", strings.Split(node.GrpcAddress, ":")[0], 52000+node.ID)
	return node, httpAddress
}

func (op *Operator) migrate(ctx context.Context) error {

	fromNode, fromHttpAddress := getRandomNodeHTTPServer(op.fromShard)
	toNode, toHttpAddress := getRandomNodeHTTPServer(op.toShard)
	op.fromNode, op.toNode = fromNode, toNode
	fromConn, err := grpc.NewClient(
		op.fromNode.GrpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024),
			grpc.MaxCallSendMsgSize(64*1024*1024),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to source node: %v", err)
	}
	defer fromConn.Close()
	fromClient := pb.NewNodeRouterClient(fromConn)

	toConn, err := grpc.NewClient(
		op.toNode.GrpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to destination node: %v", err)
	}
	defer toConn.Close()
	toClient := pb.NewNodeRouterClient(toConn)

	// Send migration requests to both nodes
	var pbKeyRanges []*pb.KeyRange
	for _, kr := range op.keyRanges {
		pbKeyRanges = append(pbKeyRanges, &pb.KeyRange{
			Start: kr.Start,
			End:   kr.End,
		})
	}

	req := &pb.MigrateRequest{
		TaskId:        op.taskId,
		FromClusterId: op.fromShard.ShardID,
		ToClusterId:   op.toShard.ShardID,
		Address:       fromHttpAddress,
		KeyRanges:     pbKeyRanges,
	}
	resp, err := toClient.StartMigration(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to start migration on destination node: %v", err)
	}
	log.Println(string(resp.Data))

	req.Address = toHttpAddress
	resp, err = fromClient.StartMigration(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to start migration on source node: %v", err)
	}
	log.Println(string(resp.Data))

	return nil
}

func (op *Operator) cancelMigration() error {
	fromConn, err := grpc.NewClient(
		op.fromNode.GrpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to source node: %v", err)
	}
	defer fromConn.Close()
	fromClient := pb.NewNodeRouterClient(fromConn)

	toConn, err := grpc.NewClient(
		op.toNode.GrpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to destination node: %v", err)
	}
	defer toConn.Close()
	toClient := pb.NewNodeRouterClient(toConn)

	// Send cancellation requests to both nodes
	cancelReq := &pb.CancelMigrationRequest{
		TaskId: op.taskId,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Cancel on source node
	_, err = fromClient.CancelMigration(ctx, cancelReq)
	if err != nil {
		log.Printf("Error cancelling migration on source node: %v", err)
		// Continue to cancel on destination even if source fails
	}

	// Cancel on destination node
	_, err = toClient.CancelMigration(ctx, cancelReq)
	if err != nil {
		return fmt.Errorf("error cancelling migration on destination node: %v", err)
	}

	log.Printf("Migration cancelled between shards %d and %d", op.fromShard.ShardID, op.toShard.ShardID)
	return nil
}

func (op *Operator) finishMigration() error {
	fromConn, err := grpc.NewClient(
		op.fromNode.GrpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to source node: %v", err)
	}
	defer fromConn.Close()
	fromClient := pb.NewNodeRouterClient(fromConn)

	toConn, err := grpc.NewClient(
		op.toNode.GrpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to destination node: %v", err)
	}
	defer toConn.Close()
	toClient := pb.NewNodeRouterClient(toConn)

	// Send finish requests to both nodes
	finishReq := &pb.ApplyMigrationRequest{
		TaskId: op.taskId,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Finish on source node
	_, err = fromClient.ApplyMigration(ctx, finishReq)
	if err != nil {
		log.Printf("Error finishing migration on source node: %v", err)
		// Continue to finish on destination even if source fails
	}

	// Finish on destination node
	_, err = toClient.ApplyMigration(ctx, finishReq)
	if err != nil {
		return fmt.Errorf("error finishing migration on destination node: %v", err)
	}

	if op.done != nil {
		close(op.done)
	}

	return nil
}
