package pkg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unicode"
)

// Verify package
func Verify(bs []byte) error {
	// Check length
	if len(bs) < MinimumPackageLength {
		err := fmt.Errorf("Package length is less than %v", MinimumPackageLength)
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

// VerifyVIN verify if vin only contains upper letter and number
func VerifyVINunicode(vin []byte) error {
	for _, v := range vin {
		r := bytes.Runes([]byte{v})[0]
		if !(unicode.IsUpper(r) || unicode.IsNumber(r)) {
			err := fmt.Errorf("invalid vin")
			return err
		}
	}
	return nil
}

var (
	byteA    = []byte("A")[0]
	byteZ    = []byte("Z")[0]
	byteZero = []byte("0")[0]
	byteNine = []byte("9")[0]
)

// VerifyVIN verify if vin only contains upper letter and number
// https://play.golang.org/p/zC7qoS5JZ2e
// "A" = 65, "Z" = 90, "0" = 48, "9" = 57
func VerifyVINascii(vin []byte) error {
	for _, b := range vin {
		switch {
		case b >= 65 && b <= 90:
		case b >= 48 && b <= 57:
		default:
			//fmt.Println("b is:", b)
			err := fmt.Errorf("invalid vin")
			//panic(err)
			//log.Error().Msgf("invalid vin:%s\n", vin)
			return err
		}
	}
	return nil
}
