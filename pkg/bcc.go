package pkg

import (
	"errors"
)


func BCC(bs []byte) (checksum byte) {
	for _, b := range bs {
		checksum ^= b
	}
	return
}

func CheckBCC(bs []byte) (err error) {
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
