// Save parsed data to hbase1
package hbase1

import (
	"fmt"
	"time"

	"github.com/matryer/try"
	"github.com/rederry/goh"
	"github.com/rederry/goh/Hbase"

	"github.com/rs/zerolog/log"
)

type HConfig struct {
	TableName string // 表名
	Addr      string // thrift Addr
	BatchSize int    // row batch size
}

type BatchClient struct {
	*goh.HClient
	*HConfig
	rowBatches []*Hbase.BatchMutation
	saveTime   time.Time
}

func NewBatchClient(hc *HConfig) (*BatchClient, error) {
	client, err := NewClient(hc.Addr)
	if err != nil {
		return nil, err
	}
	bc := &BatchClient{
		HClient:    client,
		HConfig:    hc,
		rowBatches: make([]*Hbase.BatchMutation, 0, hc.BatchSize),
		saveTime:   time.Now(),
	}
	return bc, err
}

func NewClient(addr string) (*goh.HClient, error) {
	client, err := goh.NewTcpClient(addr, goh.TBinaryProtocol, false)
	if err != nil {
		return client, err
	}

	if err = client.Open(); err != nil {
		return client, err
	}
	log.Info().Msgf("Connect to thrift:%s", addr)
	return client, err
}

func GenBatchMutation(rowKey []byte, data map[string][]byte) *Hbase.BatchMutation {
	ms := GenMutation(data)
	return goh.NewBatchMutation(rowKey, ms)
}

func GenMutation(data map[string][]byte) []*Hbase.Mutation {
	ms := make([]*Hbase.Mutation, len(data))
	var i uint = 0
	for k, v := range data {
		ms[i] = goh.NewMutation(k, v)
		i = i + 1
	}
	return ms
}

func (bc *BatchClient) batchSave(table string) {
	if len(bc.rowBatches) == 0 {
		return
	}
	save := func(attempt int) (bool, error) {
		err := bc.MutateRows(table, bc.rowBatches, nil)
		if err != nil {
			log.Error().Msgf("failed to save data to hbase,error is: %v, table name is: %s, retry...%d", err, table, attempt)
			var content string
			for _, row := range bc.rowBatches {
				k := string(row.Row)
				content = content + fmt.Sprintf("     %s    ", k)
			}
			log.Info().Msgf("failed to save data to hbase, content is: %s", content)
			bc.HClient, _ = NewClient(bc.Addr)
		}
		return attempt < 5, err // try 5 times
	}
	if err := try.Do(save); err != nil {
		log.Error().Msgf("save error: %v", err)
	}
	bc.rowBatches = bc.rowBatches[:0]
}

func (bc *BatchClient) BatchSaveData(batch *Hbase.BatchMutation) {
	bc.rowBatches = append(bc.rowBatches, batch)
	if len(bc.rowBatches) >= bc.BatchSize {
		bc.batchSave(bc.TableName)
	}
}

func (bc *BatchClient) BatchForceSave() {
	bc.batchSave(bc.TableName)
}

func (bc *BatchClient) SaveData(rowKey []byte, ms []*Hbase.Mutation) {
	save := func(attempt int) (bool, error) {
		err := bc.MutateRow(bc.TableName, rowKey, ms, nil)
		if err != nil {
			log.Error().Msgf("failed to update status, tableName: %s, retry...%d", err, bc.TableName, attempt)
			bc.HClient, _ = NewClient(bc.Addr)
		}
		return attempt < 5, err // try 5 times
	}
	if err := try.Do(save); err != nil {
		log.Fatal().Err(err)
	}
}

func (bc *BatchClient) Handle(commandKey string, result string) {
	dataMap := make(map[string][]byte)
	dataMap["cf:command_procedure_status"] = []byte(result)

	if bc.BatchSize > 0 {
		m := GenBatchMutation([]byte(commandKey), dataMap)
		bc.BatchSaveData(m)
	} else {
		ms := GenMutation(dataMap)
		bc.SaveData([]byte(commandKey), ms)
	}
}
