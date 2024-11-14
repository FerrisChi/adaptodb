package client

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func main() {
	params := RequestParams{
		Operation: q.URL.Query().Get("op"),
		Key:       q.URL.Query().Get("key"),
		Value:     q.URL.Query().Get("value"),
	}

	if params.Operation == "" || params.Key == "" {
		http.Error(w, "Missing required parameters: op and key", http.StatusBadRequest)
		return
	}
	switch params.Operation {
	case OpRead:
		if params.Key == "" {
			http.Error(w, "Missing key parameter for read operation", http.StatusBadRequest)
			return
		}

		log.Printf("Reading key %s from shard %d", params.Key, shardID)

		// request the corresponding shard

		// value, err := r.controller.Read(params.Key)
		// if err != nil {
		// 	http.Error(w, fmt.Sprintf("Read error: %v", err), http.StatusInternalServerError)
		// 	return
		// }
		log.Printf("Read key %s", params.Key)
		json.NewEncoder(w).Encode(map[string]string{"value": "dummy-value"})

	case OpWrite:
		if params.Value == "" || params.Key == "" {
			http.Error(w, "Missing parameter for write operation", http.StatusBadRequest)
			return
		}
		// err := r.controller.Write(params.Key, params.Value)
		// if err != nil {
		// 	http.Error(w, fmt.Sprintf("Write error: %v", err), http.StatusInternalServerError)
		// 	return
		// }
		log.Printf("Write key %s with value %s", params.Key, params.Value)
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, fmt.Sprintf("Invalid operation: %s", params.Operation), http.StatusBadRequest)
	}
}
