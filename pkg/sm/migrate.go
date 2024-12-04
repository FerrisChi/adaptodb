package sm

import (
	"adaptodb/pkg/schema"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const chunkSize = 1024 * 64 // 64KB chunks

type DataChunk struct {
	KeyCount uint32
	Data     []byte
}

func startTransfer(taskId uint64, toAddress string, krs []schema.KeyRange, data *sync.Map) error {
	// Use http:// prefix for initial connection
	wsURL := fmt.Sprintf("ws://%s/transfer?taskId=%d", toAddress, taskId)

	// Set up dialer with proper headers and protocols
	dialer := websocket.Dialer{}

	// Add required headers
	headers := http.Header{}

	// Attempt connection with retry logic
	var ws *websocket.Conn
	var resp *http.Response
	var err error

	for retries := 0; retries < 3; retries++ {
		ws, resp, err = dialer.Dial(wsURL, headers)
		if err == nil {
			break
		}
		log.Printf("Connection %s attempt %d failed: %v", wsURL, retries+1, err)
		time.Sleep(time.Second * 1)
	}

	if err != nil {
		if resp != nil {
			log.Printf("WebSocket handshake failed with status %d", resp.StatusCode)
		}
		return fmt.Errorf("failed to connect to %s: %v", toAddress, err)
	}
	defer ws.Close()

	log.Println("Transfering data to", toAddress)

	chunk := make([]byte, 0, chunkSize)
	count := uint32(0)

	flushChunk := func() error {
		if len(chunk) == 0 {
			return nil
		}
		// Send count followed by data
		header := make([]byte, 4)
		binary.LittleEndian.PutUint32(header, count)
		err := ws.WriteMessage(websocket.BinaryMessage, append(header, chunk...))
		chunk = chunk[:0]
		count = 0
		return err
	}

	for _, kr := range krs {
		data.Range(func(key, value interface{}) bool {
			k := key.(string)
			if k >= kr.Start && k < kr.End {
				v := value.(string)
				kLen := uint16(len(k))
				vLen := uint16(len(v))
				entrySize := 4 + len(k) + len(v) // 2 bytes for each length + data sizes

				if entrySize > chunkSize {
					log.Printf("key-value pair too large to fit in chunk: %s", k)
					return false
				}

				if len(chunk)+entrySize > chunkSize {
					if err := flushChunk(); err != nil {
						log.Printf("error flushing chunk: %v", err)
						return false
					}
				}

				chunk = append(chunk, make([]byte, 2)...)
				binary.LittleEndian.PutUint16(chunk[len(chunk)-2:], kLen)
				chunk = append(chunk, []byte(k)...)
				chunk = append(chunk, make([]byte, 2)...)
				binary.LittleEndian.PutUint16(chunk[len(chunk)-2:], vLen)
				chunk = append(chunk, []byte(v)...)
				count++
			}
			return true
		})
	}

	err = flushChunk()
	if err != nil {
		return err
	}
	log.Println("Data transfer complete", toAddress)

	return nil
}
