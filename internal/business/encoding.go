package business

import (
	"strconv"

	"github.com/highcumontoa/async-event-pipeline-go/internal/pipeline"
)

func readBalance(tx pipeline.Tx, key string) int64 {
	raw, ok := tx.Get(key)
	if !ok {
		return 0
	}
	v, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func encodeInt(v int64) []byte {
	return []byte(strconv.FormatInt(v, 10))
}

// ParseBalance decodes a balance value read from a pipeline state snapshot.
func ParseBalance(raw string) (int64, error) {
	return strconv.ParseInt(raw, 10, 64)
}
