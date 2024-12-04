package main

import (
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
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
	keyCharset    = "abcdefghijklmnopqrstuvwxyz"
	valueCharset  = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	WRITE_TIME    = 60
	INTERVAL_TIME = 5
	NUM_WORKERS   = 512
	LENGTH        = 4096
	// length := 32
	// length := 4096
	// length := 8192
	// length := 32768
	// length := 524288
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
	// length := int(math.Floor(math.Exp(rand.Float64() * math.Log(1000))))

	b := make([]byte, LENGTH)
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
	// for i := 0; i < 10; i++ {
	// 	key := generateKey()
	// 	value := generateValue()
	// 	status, err := write(key, value, meta)
	// 	if err != nil || status == 0 {
	// 		atomic.AddUint64(&metrics.failedWrites, 1)
	// 		// log.Printf("Worker %d: Write error: %v", id, err)
	// 		continue
	// 	}
	// 	atomic.AddUint64(&metrics.successfulWrites, 1)
	// 	testKeys = append(testKeys, key)
	// }

	stopWriteCh := make(chan struct{})
	go func() {
		time.Sleep(WRITE_TIME * time.Second)
		close(stopWriteCh)
	}()

writeLoop:
	for {
		select {
		case <-stopWriteCh:
			break writeLoop
		default:
			key := generateKey()
			value := generateValue()
			status, err := write(key, value, meta)
			if err != nil || status != 1 {
				atomic.AddUint64(&metrics.failedWrites, 1)
				// log.Printf("Worker %d: Write error: %v", id, err)
				continue
			}
			atomic.AddUint64(&metrics.successfulWrites, 1)
			testKeys = append(testKeys, key)
		}
	}

	time.Sleep(INTERVAL_TIME * time.Second)

	for {
		select {
		case <-stopCh:
			return
		default:
			key := testKeys[rand.Intn(len(testKeys))]
			status, err := read(key, meta)
			if err != nil || status == 0 {
				atomic.AddUint64(&metrics.failedReads, 1)
				// log.Printf("Worker %d: Read error: %v", id, err)
				continue
			}
			atomic.AddUint64(&metrics.successfulReads, 1)
		}
	}
}

type MetricsRecord struct {
	timestamp        time.Time
	successfulReads  uint64
	failedReads      uint64
	successfulWrites uint64
	failedWrites     uint64
	readSuccessRate  float64
	writeSuccessRate float64
	totalOps         uint64
}

func writeMetricsToFile(record MetricsRecord, writer *csv.Writer) error {
	row := []string{
		record.timestamp.Format(time.RFC3339),
		strconv.FormatUint(record.successfulReads, 10),
		strconv.FormatUint(record.failedReads, 10),
		strconv.FormatUint(record.successfulWrites, 10),
		strconv.FormatUint(record.failedWrites, 10),
		strconv.FormatFloat(record.readSuccessRate, 'f', 2, 64),
		strconv.FormatFloat(record.writeSuccessRate, 'f', 2, 64),
		strconv.FormatUint(record.totalOps, 10),
	}
	return writer.Write(row)
}

func createMetricsReporter(metrics *Metrics, stopCh chan struct{}) {
	// Open metrics file
	file, err := os.OpenFile("metrics.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Failed to open metrics file:", err)
	}

	writer := csv.NewWriter(file)

	// Write header
	header := []string{
		"Timestamp",
		"Successful Reads",
		"Failed Reads",
		"Successful Writes",
		"Failed Writes",
		"Read Success Rate (%)",
		"Write Success Rate (%)",
		"Total Operations",
	}
	if err := writer.Write(header); err != nil {
		log.Fatal("Failed to write header:", err)
	}

	ticker := time.NewTicker(1 * time.Second)
	go func() {
		defer file.Close()
		defer writer.Flush()

		for {
			select {
			case <-ticker.C:
				reads := atomic.SwapUint64(&metrics.successfulReads, 0)
				readFails := atomic.SwapUint64(&metrics.failedReads, 0)
				writes := atomic.SwapUint64(&metrics.successfulWrites, 0)
				writeFails := atomic.SwapUint64(&metrics.failedWrites, 0)

				totalReads := reads + readFails
				totalWrites := writes + writeFails
				totalOps := totalReads + totalWrites

				var readSuccessRate, writeSuccessRate float64
				if totalReads > 0 {
					readSuccessRate = float64(reads) / float64(totalReads) * 100
				}
				if totalWrites > 0 {
					writeSuccessRate = float64(writes) / float64(totalWrites) * 100
				}

				// Create metrics record
				record := MetricsRecord{
					timestamp:        time.Now(),
					successfulReads:  reads,
					failedReads:      readFails,
					successfulWrites: writes,
					failedWrites:     writeFails,
					readSuccessRate:  readSuccessRate,
					writeSuccessRate: writeSuccessRate,
					totalOps:         totalOps,
				}

				// Write to file
				if err := writeMetricsToFile(record, writer); err != nil {
					log.Printf("Failed to write metrics: %v", err)
				}
				writer.Flush()

				// Print to console as before
				fmt.Printf("=== Metrics Report ===\n")
				fmt.Printf("Reads - Success: %d, Failed: %d, Success Rate: %.2f%%\n",
					reads, readFails, readSuccessRate)
				fmt.Printf("Writes - Success: %d, Failed: %d, Success Rate: %.2f%%\n",
					writes, writeFails, writeSuccessRate)
				fmt.Printf("Total Ops: %d\n", totalOps)
				fmt.Println("==================")

			case <-stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

func main() {

	// Remove the tmp directory if it exists
	if err := os.RemoveAll("tmp"); err != nil {
		log.Fatalf("Failed to remove tmp directory: %v", err)
	}
	cmd := exec.Command("./kill-ports.sh")
	if err := cmd.Run(); err != nil {
		log.Fatalf("Failed to kill AdaptoDB: %v", err)
	}

	// Start the AdaptoDB server
	cmd = exec.Command("./bin/release/adaptodb")
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start AdaptoDB: %v", err)
	}
	defer func() {
		cmd.Process.Signal(syscall.SIGINT)
		cmd.Wait()
	}()

	fmt.Print("Waiting for AdaptoDB init\n")

	time.Sleep(schema.ADAPTODB_LAUNCH_TIMEOUT) // Wait for the server to start

	metrics := &Metrics{}
	stopCh := make(chan struct{})

	// Start metrics reporter
	createMetricsReporter(metrics, stopCh)

	meta := NewMetadata("127.0.0.1:60081")

	defer meta.cleanUp()

	if err := meta.getShardConfig(); err != nil {
		log.Fatal(err)
	}

	// Start workers with metrics
	log.Println("Starting workers...")
	var wg sync.WaitGroup
	for i := 0; i < NUM_WORKERS; i++ {
		wg.Add(1)
		go worker(i, meta, metrics, &wg, stopCh)
	}
	log.Println(NUM_WORKERS, "Workers started")

	go func(stopCh chan struct{}) {
		ticker := time.NewTicker(10 * time.Second)
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
	go func() {
		time.Sleep((WRITE_TIME*2 + INTERVAL_TIME) * time.Second)
		sigCh <- os.Interrupt
	}()
	// signal.Notify(sigCh, os.Interrupt)

	<-sigCh
	log.Println("Stopping workers...")
	close(stopCh)
	wg.Wait()

}
