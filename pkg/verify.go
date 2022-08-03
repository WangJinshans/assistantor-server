package pkg

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var DefaultUid = "0000000000"

func Verify(bs []byte) error {
	// Check length
	if len(bs) < 25 {
		err := fmt.Errorf("Package length is less than %v", 25)
		return err
	}

	// Check start characters
	if !bytes.Equal(bs[:2], []byte("##")) {
		err := fmt.Errorf("Start characters does not starts with ##")
		return err
	}

	// Check payload length
	payloadLen := int(binary.BigEndian.Uint16(bs[22:24]))
	if len(bs) != 24+payloadLen+1 {
		err := fmt.Errorf("The declared payload length[%d] does not match the actual[%d], length field: %x", payloadLen, len(bs)-25, bs[22:24])
		return err
	}

	// Check checksum
	if bs[len(bs)-1] != BCC(bs[2:len(bs)-1]) {
		err := fmt.Errorf("Checksum invalid")
		return err
	}

	return nil
}

func VerifyUid(bs []byte) (err error) {
	return
}
