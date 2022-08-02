package pkg

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"gotest.tools/assert"
)

func TestCryptoRandomString(t *testing.T) {
	count := 100000
	table := make(map[string]bool)
	for i := 0; i < count; i++ {
		s := CryptoRandString(17)
		table[s] = true
	}
	assert.Equal(t, count, len(table))
}

func TestRandomString(t *testing.T) {
	count := 100000
	table := make(map[string]bool)
	for i := 0; i < count; i++ {
		s := RandString(17)
		table[s] = true
	}
	assert.Equal(t, count, len(table))
}

func TestParallelRandomString(t *testing.T) {
	count := 2000
	parallel := 100
	var wg sync.WaitGroup
	resultChan := make(chan []string, parallel)
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			var sSlice []string
			for i := 0; i < count; i++ {
				s := RandString(17)
				sSlice = append(sSlice, s)
			}
			select {
			case resultChan <- sSlice:
			case <-time.After(time.Second):
				panic("timeout")
			}
			wg.Done()
		}()
	}

	wg.Wait()
	close(resultChan)

	sMap := make(map[string]bool)
	for sSlice := range resultChan {
		for _, s := range sSlice {
			sMap[s] = true
		}
	}
	log.Info().Msgf("len of map: %d", len(sMap))
	assert.Equal(t, count*parallel, len(sMap))
}

func TestParallelRandomStringManualSeed(t *testing.T) {
	count := 2000
	parallel := 100
	var wg sync.WaitGroup
	resultChan := make(chan []string, parallel)
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			var sSlice []string
			r := rand.New(rand.NewSource(42))
			for i := 0; i < count; i++ {
				s := RandStringManualSeed(17, r)
				sSlice = append(sSlice, s)
			}
			select {
			case resultChan <- sSlice:
			case <-time.After(time.Second):
				panic("timeout")
			}
			wg.Done()
		}()
	}

	wg.Wait()
	close(resultChan)

	sMap := make(map[string]bool)
	for sSlice := range resultChan {
		for _, s := range sSlice {
			sMap[s] = true
		}
	}
	log.Info().Msgf("len of map: %d", len(sMap))
}