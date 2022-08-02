package pkg

import (
	"encoding/hex"
	"errors"
	"github.com/rs/zerolog/log"
)

// BCC calculate bcc of pakcage
// We use native golang implement since it has better
// performance than cgo version and asm version
func BCC(bs []byte) (checksum byte) {
	for _, b := range bs {
		checksum ^= b
	}
	return
}

func CheckBCC(bs []byte) error {
	if bs[len(bs)-1] != BCC(bs[2:len(bs)-1]) {
		log.Info().Msgf("bcc err, hex is: %s", hex.EncodeToString(bs))
		return errors.New("bcc err")
	}
	return nil
}

func CheckBCCV1(bs []byte) (err error) {
	var checksum byte
	for _, b := range bs {
		checksum ^= b
	}
	if checksum == 0 {
		return
	}
	err = errors.New("bcc err")
	return
}
