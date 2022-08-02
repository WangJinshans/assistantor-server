package pkg

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	vin     = RandString(17)
	creator = NewCreator(vin)
)

func init() {
	log.Logger = zerolog.New(os.Stderr).With().Caller().Timestamp().Logger()
}

func TestRandTimeCalibrationPkg(t *testing.T) {
	bs := creator.RandTimeCalibrationPkg()
	t.Logf("%x", bs)

	if len(bs) != 25 {
		t.Errorf("Package length not valid: %v", len(bs))
	}
}

func TestRandRealtimePkg(t *testing.T) {
	bs := creator.RandRealtimePkg()
	t.Logf("%x", bs)
}

func TestRandCommand(t *testing.T) {
	bs := creator.RandCommand()
	t.Logf("command: %x", bs)

	id := GetCommandID(bs)
	t.Logf("command: %x, id: %s", bs, id)

	response := GetSuccessResponse(bs)
	t.Logf("command: %x, id: %s", bs, id)
	t.Logf("response: %x", response)
}

func TestGetResponse(t *testing.T) {
	bs := creator.RandCommand()
	t.Logf("command: %x", bs)

	response := GetResponse(bs)
	t.Logf("bs: %x", bs)
	t.Logf("response: %x", response)
}
