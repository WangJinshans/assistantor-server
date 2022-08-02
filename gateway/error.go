package gateway

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"time"
)

// FormatErrorMsg format error to byte slice
func FormatErrorMsg(source, content, ip, errorMessage string) []byte {
	t := time.Now().Unix()

	data := map[string]interface{}{
		"source":        source,
		"message":       content,
		"ip":            ip,
		"error_message": errorMessage,
		"create_time":   t,
	}
	msg, err := json.Marshal(data)
	if err != nil {
		log.Error().Msg(err.Error())
	}
	return msg
}
