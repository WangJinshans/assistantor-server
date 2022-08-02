package util

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
	"reflect"
	"time"
)

func GetRedisClient(host string, port int, password string, db int) *redis.Client {
	return GetRedisClientWithTimeOut(host, port, password, db, 0)
}

//set read time out
func GetRedisClientWithTimeOut(host string, port int, password string, db int, readTimeout int) *redis.Client {
	redisAddr := fmt.Sprintf("%v:%v", host, port)
	options := &redis.Options{
		Addr:       redisAddr,
		Password:   password,
		DB:         db,
		MaxRetries: 3,
	}
	if readTimeout > 0 {
		options.ReadTimeout = time.Duration(readTimeout) * time.Millisecond
	}
	redisClient := redis.NewClient(options)
	log.Info().Msgf("connect to redis at %v", redisAddr)
	return redisClient
}

func ScanKeys(searchKey string, redisClient *redis.Client) (resultList []string, err error) {

	var cursor uint64
	var keyList []string
	var scanResult []string
	var elementList interface{}

	if searchKey == "" {
		err = errors.New("empty search key")
		return
	}

	for {
		scanResult, cursor, err = redisClient.Scan(cursor, searchKey, 1000).Result() // 可能会重复
		//commandCacheList, err := redisClient.Keys(commandCacheKey).Result()    // keys * 会阻塞其他查询
		if len(scanResult) > 0 {
			keyList = append(keyList, scanResult...)
		}
		if err != nil {
			log.Error().Msgf("get connection error: %v", err)
		}
		if cursor == 0 {
			break
		}
	}

	elementList, err = RemoveDuplicateElement(keyList) //去重
	if !reflect.ValueOf(elementList).IsNil() {
		resultList = elementList.([]string)
	}
	return
}
