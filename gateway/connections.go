package gateway

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurnishCN/gateway-go/pkg"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
)

const (
	// RedisKeyConnectionTable record all connected with its id
	//
	// key: connID
	//
	// value: last_update_time + host
	RedisKeyConnectionTable = "connection_table"

	// RedisKeyAliveHosts record all alive gateway instance
	//
	// key: host
	//
	// value: last_update_time
	RedisKeyAliveHosts = "alive_hosts"
)

var (
	HostIP string
)

func init() {
	HostIP = os.Getenv("IP")
}

// UpdateConn update a connection's info in redis
// currently used in debug scripts
func UpdateConn(redisClient *redis.Client, id string) {
	log.Info().Msgf("update: %s", id)
	pipe := redisClient.Pipeline()
	curr := time.Now().Unix()

	v := fmt.Sprintf("%v:%v", curr, HostIP)
	pipe.HSet(RedisKeyConnectionTable, id, v)

	reverseKey := fmt.Sprintf("connections:%v", HostIP)
	pipe.HSet(reverseKey, id, curr)

	_, err := pipe.Exec()
	if err != nil {
		log.Error().Err(err)
	}
}

// ReportAlive send alive infomation to redis
func ReportAlive(redisClient *redis.Client) {
	for {
		curr := time.Now().Unix()
		cmd := redisClient.HSet(RedisKeyAliveHosts, HostIP, curr)
		_, err := cmd.Result()
		if err != nil {
			log.Error().Msgf("redis error: %s", err)
		}
		time.Sleep(10 * time.Second)
	}
}

// GetConnections get all connections
func GetConnections(redisClient *redis.Client, filteAlive bool) map[string]string {
	curr := time.Now()

	conns := redisClient.HGetAll(RedisKeyConnectionTable)
	result, err := conns.Result()
	if err != nil {
		panic(err)
	}
	log.Info().Msgf("result: %v", result)

	// filter invalid key
	filterdResult := make(map[string]string)
	for key, val := range result {
		err = pkg.VerifyVINascii([]byte(key))
		if err == nil {
			filterdResult[key] = val
		}
	}
	log.Info().Msgf("filted result: %v", filterdResult)

	if !filteAlive {
		return filterdResult
	}

	aliveConns := make(map[string]string)
	var inactivateConns []string
	for key, val := range filterdResult {
		ts := strings.Split(val, ":")[0]

		i, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			panic(err)
		}
		tm := time.Unix(i, 0)
		if curr.Sub(tm) < 60*time.Second {
			aliveConns[key] = val
		} else {
			inactivateConns = append(inactivateConns, key)
		}
	}
	redisClient.HDel(RedisKeyConnectionTable, inactivateConns...)
	return aliveConns
}

// GetConnectionsByHost get connections by given filter
// Pass host with empty string to obtain all connections
func GetConnectionsByHost(redisClient *redis.Client, host string, filteAlive bool) map[string]string {
	curr := time.Now()

	reverseKey := fmt.Sprintf("connections:%v", host)

	conns := redisClient.HGetAll(reverseKey)
	result, err := conns.Result()
	if err != nil {
		panic(err)
	}

	if !filteAlive {
		return result
	}
	aliveConns := make(map[string]string)
	for key, val := range result {
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			panic(err)
		}
		tm := time.Unix(i, 0)
		if curr.Sub(tm) < 60*time.Second {
			aliveConns[key] = val
		}
	}
	return aliveConns
}

// GetHosts get all host
func GetHosts(redisClient *redis.Client, filteAlive bool) map[string]string {
	curr := time.Now()

	conns := redisClient.HGetAll(RedisKeyAliveHosts)
	result, err := conns.Result()
	if err != nil {
		panic(err)
	}

	if !filteAlive {
		return result
	}

	aliveHosts := make(map[string]string)
	for key, val := range result {
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			panic(err)
		}
		tm := time.Unix(i, 0)
		if curr.Sub(tm) < 60*time.Second {
			aliveHosts[key] = val
		}
	}
	return aliveHosts
}
