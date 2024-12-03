package main

import (
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Metadata struct {
	meta        map[uint64]ShardInfo
	metaAddress string
	mutex       sync.RWMutex
	conn        map[string]*grpc.ClientConn
	connMux     map[string]*sync.RWMutex
}

func NewMetadata(metaAddress string) *Metadata {
	connMux := make(map[string]*sync.RWMutex)
	connMux[metaAddress] = &sync.RWMutex{}

	return &Metadata{
		meta:        make(map[uint64]ShardInfo),
		metaAddress: metaAddress,
		conn:        make(map[string]*grpc.ClientConn),
		connMux:     connMux,
	}
}

type ShardInfo struct {
	KeyRanges []schema.KeyRange
	Addresses []string
}

type Metrics struct {
	successfulReads  uint64
	failedReads      uint64
	successfulWrites uint64
	failedWrites     uint64
}

const (
	keyCharset   = "abcdefghijklmnopqrstuvwxyz"
	valueCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	READ_RATIO   = 0.75
)

func generateKey() string {
	length := int(math.Floor(math.Exp(rand.Float64() * math.Log(50))))
	b := make([]byte, length)
	for i := range b {
		b[i] = keyCharset[rand.Intn(len(keyCharset))]
	}
	return string(b)
}

func generateValue() string {
	length := int(math.Floor(math.Exp(rand.Float64() * math.Log(1000))))
	b := make([]byte, length)
	for i := range b {
		b[i] = valueCharset[rand.Intn(len(valueCharset))]
	}
	return string(b)
}

func (m *Metadata) getConn(addr string) (*grpc.ClientConn, error) {
	m.connMux[addr].RLock()
	if m.conn[addr] != nil {
		m.connMux[addr].RUnlock()
		return m.conn[addr], nil
	}
	m.connMux[addr].RUnlock()

	m.connMux[addr].Lock()
	defer m.connMux[addr].Unlock()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	if m.conn[addr] != nil {
		return m.conn[addr], nil
	}
	m.conn[addr] = conn
	return conn, nil
}

func (m *Metadata) cleanUp() {
	for _, conn := range m.conn {
		conn.Close()
	}
}

func (m *Metadata) getShardConfig() error {
	conn, err := m.getConn(m.metaAddress)
	if err != nil {
		return err
	}

	client := pb.NewShardRouterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
	defer cancel()

	resp, err := client.GetConfig(ctx, &pb.GetConfigRequest{})
	if err != nil {
		log.Fatalf("Failed to get shard mapping: %v", err)
	}

	// convert the response to []schema.KeyRange
	newMeta := make(map[uint64]ShardInfo)
	for shardId, keyRangeList := range resp.GetShardMap() {
		krs := make([]schema.KeyRange, 0, len(keyRangeList.GetKeyRanges()))
		for _, keyRange := range keyRangeList.GetKeyRanges() {
			krs = append(krs, schema.KeyRange{
				Start: keyRange.GetStart(),
				End:   keyRange.GetEnd(),
			})
		}
		addrs := make([]string, 0, len(resp.GetMembers()[shardId].GetMembers()))
		for _, member := range resp.GetMembers()[shardId].GetMembers() {
			addr := member.GetAddr()
			addrs = append(addrs, addr)
			if _, ok := m.connMux[addr]; !ok {
				m.connMux[addr] = &sync.RWMutex{}
			}
		}
		newMeta[shardId] = ShardInfo{
			KeyRanges: krs,
			Addresses: addrs,
		}
	}

	m.mutex.Lock()
	m.meta = newMeta
	m.mutex.Unlock()
	return nil
}

func (m *Metadata) getShardAddressForKey(key string) (uint64, string) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	for shardId, info := range m.meta {
		for _, kr := range info.KeyRanges {
			if key >= kr.Start && key < kr.End {
				return shardId, info.Addresses[rand.Intn(len(info.Addresses))]
			}
		}
	}
	return 0, ""
}

func write(key, value string, meta *Metadata) (uint64, error) {
	shardId, address := meta.getShardAddressForKey(key)
	conn, err := meta.getConn(address)
	if err != nil {
		return 0, err
	}
	client := pb.NewNodeRouterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
	defer cancel()
	resp, err := client.Write(ctx, &pb.WriteRequest{ClusterID: shardId, Key: key, Value: value})
	if err != nil {
		return 0, err
	}
	return resp.GetStatus(), nil
}

func read(key string, meta *Metadata) (uint64, error) {
	shardId, address := meta.getShardAddressForKey(key)
	conn, err := meta.getConn(address)
	if err != nil {
		return 0, err
	}
	client := pb.NewNodeRouterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
	defer cancel()
	resp, err := client.Read(ctx, &pb.ReadRequest{ClusterID: shardId, Key: key})
	if err != nil {
		return 0, err
	}
	return resp.GetStatus(), nil
}

func worker(id int, meta *Metadata, metrics *Metrics, wg *sync.WaitGroup, stopCh chan struct{}) {
	defer wg.Done()

	var testKeys []string
	for i := 0; i < 100; i++ {
		testKeys = append(testKeys, generateKey())
		status, err := write(testKeys[i], generateValue(), meta)
		if err != nil || status == 0 {
			atomic.AddUint64(&metrics.failedWrites, 1)
			// log.Printf("Worker %d: Write error: %v", id, err)
			continue
		}
		atomic.AddUint64(&metrics.successfulWrites, 1)
	}

	for {
		select {
		case <-stopCh:
			return
		default:
			if rand.Float64() < READ_RATIO { // 75% reads
				key := testKeys[rand.Intn(len(testKeys))]
				status, err := read(key, meta)
				if err != nil || status == 0 {
					atomic.AddUint64(&metrics.failedReads, 1)
					// log.Printf("Worker %d: Read error: %v", id, err)
					continue
				}
				atomic.AddUint64(&metrics.successfulReads, 1)
			} else { // 25% writes
				var key string
				if len(testKeys) < 100 || rand.Float64() < 0.2 {
					key = generateKey()
				} else {
					key = testKeys[rand.Intn(len(testKeys))]
				}
				value := generateValue()
				status, err := write(key, value, meta)
				if err != nil || status != 1 {
					atomic.AddUint64(&metrics.failedWrites, 1)
					// log.Printf("Worker %d: Write error: %v", id, err)
					continue
				}
				atomic.AddUint64(&metrics.successfulWrites, 1)
			}
		}
	}
}

func createMetricsReporter(metrics *Metrics, stopCh chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				reads := atomic.SwapUint64(&metrics.successfulReads, 0)
				readFails := atomic.SwapUint64(&metrics.failedReads, 0)
				writes := atomic.SwapUint64(&metrics.successfulWrites, 0)
				writeFails := atomic.SwapUint64(&metrics.failedWrites, 0)

				totalReads := reads + readFails
				totalWrites := writes + writeFails

				var readSuccessRate, writeSuccessRate float64
				if totalReads > 0 {
					readSuccessRate = float64(reads) / float64(totalReads) * 100
				}
				if totalWrites > 0 {
					writeSuccessRate = float64(writes) / float64(totalWrites) * 100
				}

				fmt.Printf("=== Metrics Report ===\n")
				fmt.Printf("Reads - Success: %d, Failed: %d, Success Rate: %.2f%%\n",
					reads, readFails, readSuccessRate)
				fmt.Printf("Writes - Success: %d, Failed: %d, Success Rate: %.2f%%\n",
					writes, writeFails, writeSuccessRate)
				fmt.Printf("Total Ops: %d\n", totalReads+totalWrites)
				fmt.Println("==================")
			case <-stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

func main() {
	metrics := &Metrics{}
	stopCh := make(chan struct{})

	// Start metrics reporter
	createMetricsReporter(metrics, stopCh)

	meta := NewMetadata("localhost:60081")

	defer meta.cleanUp()

	if err := meta.getShardConfig(); err != nil {
		log.Fatal(err)
	}

	// Start workers with metrics
	log.Println("Starting workers...")
	var wg sync.WaitGroup
	numWorkers := 100
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, meta, metrics, &wg, stopCh)
	}
	log.Println(numWorkers, "Workers started")

	go func(stopCh chan struct{}) {
		ticker := time.NewTicker(30 * time.Second)
		for {
			select {
			case <-stopCh:
				ticker.Stop()
				return
			case <-ticker.C:
				if err := meta.getShardConfig(); err != nil {
					log.Printf("Error refreshing metadata: %v", err)
				}
			}
		}
	}(stopCh)

	// Handle interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	<-sigCh
	log.Println("Stopping workers...")
	close(stopCh)
	wg.Wait()

}
