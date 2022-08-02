package gateway

import (
	"os"

	"github.com/rs/zerolog/log"
)

// ConfigNetworkFromEnv ...
func ConfigNetworkFromEnv() int {
	mode := os.Getenv("NETWORK_MODE")
	log.Info().Msgf("network mode: %s", mode)

	switch mode {
	case "naive":
		return NetworkModeNaive
	case "epoll":
		return NetworkModeEpoll
	default:
		return NetworkModeNaive
	}
}
