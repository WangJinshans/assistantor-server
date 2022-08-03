// HBase table client
package hbase1

import (
	"context"
	"sync"
	"time"

	"github.com/matryer/try"
	"github.com/rederry/goh"
	"github.com/rederry/goh/Hbase"
	"github.com/rs/zerolog/log"
)

const (
	defaultBufferSize     = 5000
	defaultBatchSize      = 100
	defaultMaxRetryNumber = 5
	defaultFlushInterval  = 2 * time.Second
)

type HTable struct {
	addr      string // thrift Addr
	tableName string
	*goh.HClient
	bufferSize    int // write buffer size
	batchSize     int // batch size
	mux           sync.Mutex
	writeBuffer   []HRecord
	lastFlushTime time.Time
	flashInterval time.Duration
	cancel        context.CancelFunc
}

func NewHTable(addr, tableName string) (*HTable, error) {
	client, err := NewClient(addr)
	if err != nil {
		return nil, err
	}
	ht := &HTable{
		addr:          addr,
		tableName:     tableName,
		HClient:       client,
		bufferSize:    defaultBufferSize,
		batchSize:     defaultBatchSize,
		writeBuffer:   make([]HRecord, 0),
		lastFlushTime: time.Now(),
		flashInterval: defaultFlushInterval,
	}

	ctx, cancel := context.WithCancel(context.Background())
	ht.cancel = cancel
	go ht.startFlusher(ctx)

	return ht, nil
}

func (ht *HTable) startFlusher(ctx context.Context) {
	ticker := time.NewTicker(ht.flashInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if time.Now().Sub(ht.lastFlushTime) > ht.flashInterval {
				ht.flush(false)
			}
		case <-ctx.Done():
			log.Info().Msgf("hTable(%s) flusher stopped", ht.tableName)
			return
		}
	}
}

func (ht *HTable) flush(force bool) {
	ht.mux.Lock()
	defer ht.mux.Unlock()

	bufferLen := len(ht.writeBuffer)
	log.Info().Msgf("hTable(%s) ready to flush with size %d", ht.tableName, bufferLen)

	if !force && bufferLen < ht.bufferSize && time.Now().Sub(ht.lastFlushTime) < ht.flashInterval {
		return
	}

	rowBatches := make([]*Hbase.BatchMutation, 0, bufferLen)
	for _, record := range ht.writeBuffer {
		batchMutation := GenBatchMutation(record.GetRowKey(), record.GetQualifiersMap())
		rowBatches = append(rowBatches, batchMutation)
	}

	startIndex := 0
	endIndex := ht.batchSize
	for startIndex < bufferLen {
		if endIndex >= bufferLen {
			endIndex = bufferLen
		}

		err := ht.MutateRowsWithRetry(rowBatches[startIndex:endIndex])
		if err != nil {
			log.Error().Msgf("hTable(%s) batch error: %s", ht.tableName, err)
		}

		startIndex = endIndex
		endIndex = endIndex + ht.batchSize
	}

	ht.lastFlushTime = time.Now()
	ht.writeBuffer = make([]HRecord, 0)
}

func (ht *HTable) MutateRowsWithRetry(rowBatches []*Hbase.BatchMutation) error {
	save := func(attempt int) (bool, error) {
		err := ht.MutateRows(ht.tableName, rowBatches, nil)
		if err != nil {
			log.Error().Msgf("save to hbase1 failed[%s] Retry...%d", err, attempt)
			ht.HClient, _ = NewClient(ht.addr)
		}
		return attempt < defaultMaxRetryNumber, err // try 5 times
	}
	return try.Do(save)
}

// Save record to hbase1
func (ht *HTable) Save(record HRecord) {
	ht.mux.Lock()
	ht.writeBuffer = append(ht.writeBuffer, record)
	ht.mux.Unlock()

	if len(ht.writeBuffer) >= ht.bufferSize {
		ht.flush(false)
	}
}

// Close htable and flush buffer
func (ht *HTable) Close() {
	if ht.cancel != nil {
		ht.cancel()
	}
	ht.flush(true)
	ht.HClient.Close()
	log.Info().Msgf("hTable(%s) closed", ht.tableName)
}
