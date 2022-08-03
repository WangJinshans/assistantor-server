package util

import (
	"fmt"
	"github.com/imroc/biu"
	"github.com/rs/zerolog/log"
)

func parseBinToHex(s string) (hexRes string, err error) {
	var res uint16
	err = biu.ReadBinaryString(s, &res)
	if err != nil {
		return
	}
	hexRes = fmt.Sprintf("%.4x", res)
	log.Info().Msgf("s is: %s, res is: %s", s, hexRes)
	return
}
