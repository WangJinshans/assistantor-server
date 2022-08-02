package pkg

import (
	cryptoRand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// Creator -- to create package
type Creator struct {
	VIN string

	protocol string

	bs          []byte
	RandPayload bool

	commandFlag string
	answerFlag  string
	encryptFLag string
}

func init() {
	seed := time.Now().UnixNano()
	rand.Seed(seed)
}

// NewCreator -- to create instance of Creator
func NewCreator(vin string) (c *Creator) {
	c = new(Creator)
	c.VIN = vin
	c.encryptFLag = EncryptFlagNotEncrypt
	return
}

func NewCreatorWithProtocol(vin string, protocol string) (c *Creator) {
	c = new(Creator)
	c.VIN = vin
	c.encryptFLag = EncryptFlagNotEncrypt
	c.protocol = protocol
	return
}

func (c *Creator) wrapPayload(payload []byte) []byte {
	if c.protocol == ProtocolGB17691 {
		return c.wrapPaylodGB17691(payload)
	}

	var bs []byte
	bs = append(bs, c.commandFlag...)
	bs = append(bs, c.answerFlag...)
	bs = append(bs, []byte(c.VIN)...)
	bs = append(bs, c.encryptFLag...)

	length := make([]byte, 2)
	binary.BigEndian.PutUint16(length, uint16(len(payload)))
	bs = append(bs, length...)
	bs = append(bs, payload...)

	checksum := BCC(bs)
	bs = append([]byte(StartFlag), bs...)
	bs = append(bs, checksum)
	return bs
}

func (c *Creator) wrapPaylodGB17691(payload []byte) []byte {
	var bs []byte
	bs = append(bs, c.commandFlag...)
	bs = append(bs, []byte(c.VIN)...)

	// 终端软件版本号
	bs = append(bs, 255)

	bs = append(bs, c.encryptFLag...)

	length := make([]byte, 2)
	binary.BigEndian.PutUint16(length, uint16(len(payload)))
	bs = append(bs, length...)
	bs = append(bs, payload...)

	checksum := BCC(bs)
	bs = append([]byte(StartFlag), bs...)
	bs = append(bs, checksum)
	return bs
}

// RealtimePkg -- return a realtimePkg data
func (c *Creator) RealtimePkg() []byte {
	return c.bs
}

// ResponseHeartbeat -- Response to 07pkg(Heartbeat)
func (c *Creator) ResponseHeartbeat(time []byte) []byte {
	c.commandFlag = CommandFlagHeartbeat
	c.answerFlag = AnswerFlagSuccess

	return c.wrapPayload(time)
}

// ResponseTimeCalibration -- Response (08)TimeCalibration pkg
func (c *Creator) ResponseTimeCalibration() []byte {
	t := GetTime()

	c.commandFlag = CommandFlagTimeCalibration
	c.answerFlag = AnswerFlagCommand

	return c.wrapPayload(t)
}

func (c *Creator) ResponseForward(commandFlag byte, time []byte) []byte {
	c.commandFlag = string(commandFlag)
	c.answerFlag = AnswerFlagSuccess

	return c.wrapPayload(time)
}

// RandPkg -- to generate data
func (c *Creator) RandPkg(sendRealtimePkg bool) []byte {
	n := rand.Float64()

	if n < 0.1 {
		return c.RandHeartbeatPkg()
	} else if n < 0.2 {
		return c.RandTimeCalibrationPkg()
	} else if sendRealtimePkg {
		return c.RandRealtimePkg()
	} else {
		return nil
	}
}

// RandHeartbeatPkg -- to generate HeartbeatPkg
func (c *Creator) RandHeartbeatPkg() []byte {
	t := GetTime()

	c.commandFlag = CommandFlagHeartbeat
	c.answerFlag = AnswerFlagCommand

	return c.wrapPayload(t)
}

// RandTimeCalibrationPkg -- to generate TimeCalibrationPkg
func (c *Creator) RandTimeCalibrationPkg() []byte {
	var payload []byte

	c.commandFlag = CommandFlagTimeCalibration
	c.answerFlag = AnswerFlagCommand

	return c.wrapPayload(payload)
}

// RandRealtimePkg -- to generate random length data
func (c *Creator) RandRealtimePkg() []byte {
	minLen := 10
	maxLen := 85
	n := rand.Intn(maxLen-minLen) + minLen
	payload := RandBytes(n)

	t := GetTime()
	log.Debug().Msgf("Time: %x", t)
	payload = append(t, payload...)

	c.commandFlag = CommandFlagRealtime
	c.answerFlag = AnswerFlagCommand

	return c.wrapPayload(payload)
}

// RandGuoliuRealtimePkg -- to generate  GuoliuRealtimePkg
func (c *Creator) RandGuoliuRealtimePkg() []byte {
	var payload []byte
	serialNum := RandBytes(2)
	g6ObdMsg := c.guoliuObdMsg()
	g6DataFlowMsg := guoliuDataFlowMsg()
	payload = append(payload, serialNum...)
	payload = append(payload, g6ObdMsg...)
	payload = append(payload, g6DataFlowMsg...)

	t := GetTime()
	log.Debug().Msgf("Time: %x", t)
	payload = append(t, payload...)

	var bs []byte
	bs = append(bs, "\x02"...)
	bs = append(bs, []byte("\xfe")...)
	bs = append(bs, []byte(c.VIN)...)
	// encrypt method
	bs = append(bs, "\x01"...)

	length := make([]byte, 2)
	binary.BigEndian.PutUint16(length, uint16(len(payload)))
	bs = append(bs, length...)
	bs = append(bs, payload...)

	checksum := BCC(bs)
	bs = append([]byte(StartFlag), bs...)
	bs = append(bs, checksum)
	return bs
}

func (c *Creator) RandCommand() []byte {

	log.Info().Msg("RandBytes")
	payload := RandBytes(10)
	t := GetTime()
	log.Info().Msgf("Time: %x", t)
	payload = append(t, payload...)

	c.commandFlag = CommandFlagCommand
	c.answerFlag = AnswerFlagCommand

	command := c.wrapPayload(payload)
	log.Info().Msgf("command: %x", command)
	return command
}

func GetCommandID(data []byte) []byte {
	var id []byte
	vin := data[4:21]
	t := fmt.Sprintf("%x", data[24:30])
	id = append(id, vin...)
	id = append(id, t...)
	return id
}

func GetSuccessResponse(data []byte) []byte {
	vin := data[4:21]
	t := data[24:30]

	c := NewCreator(string(vin))
	c.commandFlag = string([]byte{data[2]})
	c.answerFlag = AnswerFlagSuccess

	return c.wrapPayload(t)
}

// GetTime turn current time to gb format (6 byte slice)
func GetTime() []byte {
	t := time.Now()
	return []byte{
		byte(t.Year() - 2000),
		byte(t.Month()),
		byte(t.Day()),
		byte(t.Hour()),
		byte(t.Minute()),
		byte(t.Second()),
	}
}

// RandString return random string with length n
func RandString(n int) string {
	var letterRunes = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// RandString return random string with length n
func RandStringManualSeed(n int, manualRand *rand.Rand) string {
	var letterRunes = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[manualRand.Intn(len(letterRunes))]
	}
	return string(b)
}

// RandBytes return random byte slice with length n
func RandBytes(n int) []byte {
	bs := make([]byte, n)
	rand.Read(bs)
	return bs
}

func (c *Creator) guoliuObdMsg() []byte {
	var payload []byte
	payload = append(payload, "\x01\x00\x01"...)
	payload = append(payload, RandBytes(4)...)
	payload = append(payload, []byte(c.VIN)...)
	// software_id --18bytes
	// cvn--18bytes
	// iupr--36bytes
	data := []byte(strings.Repeat("1", 36) + strings.Repeat("0", 36))
	payload = append(payload, data...)
	faultData := "\x01\x00\x92\x34\x00"
	payload = append(payload, faultData...)
	return payload
}

func guoliuDataFlowMsg() []byte {
	var payload []byte
	payload = append(payload, "\x02"...)
	payload = append(payload, RandBytes(7)...)
	payload = append(payload, "\x00\x01"...)
	payload = append(payload, RandBytes(15)...)
	//location_flag
	payload = append(payload, "\x01"...)
	// lat and lng 、accumulated_mileage
	payload = append(payload, RandBytes(12)...)
	return payload
}

// GetCommandReply generate reply for downstream command
// currently not support dynamic command flag
func GetCommandReply(message []byte) ([]byte, error) {
	if len(message) < 31 {
		err := fmt.Errorf("command length less than 31: %d", len(message))
		return nil, err
	}

	time := message[24:30]
	vin := message[4:21]
	creator := NewCreator(string(vin))
	creator.commandFlag = CommandFlagCommand
	creator.answerFlag = AnswerFlagSuccess
	return creator.wrapPayload(time), nil
}

func GetResponse(bs []byte) []byte {
	bs[3] = 1
	bs[22] = 0
	bs[23] = 6
	bs[30] = BCC(bs[2:30])
	return bs[:31]
}

func CryptoRandString(length int) (s string) {
	for {
		nBig, err := cryptoRand.Int(cryptoRand.Reader, big.NewInt(256))
		n := int(nBig.Int64())
		if err != nil {
			panic(err)
		}
		if (n >= 65 && n <= 90) || (n >= 48 && n <= 57) {
			s += fmt.Sprintf("%d", n)
			if len(s) == length {
				return
			}
		}
	}
}
