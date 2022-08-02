package util

import (
	"github.com/rs/zerolog/log"
	"net"
	"os"
)

func GetLocalIP() string {
	addressList, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}

	for _, address := range addressList {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func GetHost() (hostName string) {
	hostName, err := os.Hostname()
	if err != nil {
		log.Error().Msgf("get host name error: %v", err)
		return
	}
	return
}
