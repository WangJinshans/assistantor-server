package pkg

import (
	"testing"

	"gotest.tools/assert"
)

var (
	vins [][]byte
)

func init() {
	// 50% valid vin, 50% invalid vin
	for i := 0; i < 10000; i++ {
		vins = append(vins, []byte(RandString(17)))
	}
	for i := 0; i < 10000; i++ {
		vins = append(vins, RandBytes(17))
	}
}

func TestVerify(t *testing.T) {
	bs := creator.RandGuoliuRealtimePkg()
	err := Verify(bs)
	assert.Assert(t, err == nil)
	t.Log(err)

	bs = []byte("##123")
	err = Verify(bs)
	assert.Assert(t, err != nil)
	t.Log(err)

	bs = RandBytes(30)
	err = Verify(bs)
	assert.Assert(t, err != nil)
	t.Log(err)

	bs = creator.RandGuoliuRealtimePkg()
	last := len(bs) - 1
	bs[last] = bs[last] - 1
	err = Verify(bs)
	assert.Assert(t, err != nil)
	t.Log(err)
}

func TestVerifyVINunicode(t *testing.T) {
	vin := []byte("TEST0717VEH000001")
	err := VerifyVINunicode(vin)
	assert.Assert(t, err == nil)

	vin = []byte("\xfeCXN2KI6LNQE52UTG")
	err = VerifyVINunicode(vin)
	assert.Assert(t, err != nil)
}

func TestVerifyVINascii(t *testing.T) {
	vin := []byte("TEST0717VEH000001")
	err := VerifyVINascii(vin)
	assert.Assert(t, err == nil)

	vin = []byte("\xfeCXN2KI6LNQE52UTG")
	err = VerifyVINascii(vin)
	assert.Assert(t, err != nil)

	vin = []byte("LcXN2KI6LNQE52UTG")
	err = VerifyVINascii(vin)
	assert.Assert(t, err != nil)
}

func BenchmarkVerifyVINunicode(b *testing.B) {
	for i := 0; i < 20000; i++ {
		VerifyVINunicode(vins[i])
	}
}

func BenchmarkVerifyVINascii(b *testing.B) {
	for i := 0; i < 20000; i++ {
		VerifyVINascii(vins[i])
	}
}
