package gateway

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"github.com/BurnishCN/gateway-go/global"

	"github.com/BurnishCN/gateway-go/pkg"
	"github.com/rs/zerolog/log"
)

// 切分后的数据
type SplitedData struct {
	messages        [][]byte // 切分后有效的消息。
	invalidMessages [][]byte // 切分后无效的数据。
	residueBytes    []byte   // 余下的字节数据
}

var gb17691StartFlag = []byte("##")
var hjStartFlag = []byte("~~") // hj其实符调整

func checkBCC(bs []byte) error {
	if bs[len(bs)-1] != pkg.BCC(bs[2:len(bs)-1]) {
		log.Info().Msgf("bcc err, hex is: %s", hex.EncodeToString(bs))
		return errors.New("bcc err")
	}
	return nil
}

// Split is the best segmentation function at present,
// and has reached the optimal speed and accuracy.
func Split(segment []byte) (messages [][]byte, residueBytes []byte, invalidMessages [][]byte) {
	var startFlag []byte
	if global.Protocol == pkg.ProtocolHJ {
		startFlag = hjStartFlag
	} else {
		startFlag = gb17691StartFlag
	}
	var invalidBuf bytes.Buffer

	maybeFakeFlag := false

	for len(segment) > 0 {
		// 1.检查最小长度， 如果长度过短，跳出循环，并清空无效数据缓冲buf。
		if len(segment) < pkg.MinimumPackageLength {
			inbs := invalidBuf.Bytes()
			if len(inbs) > 0 {
				invalidMessages = append(invalidMessages, inbs)
			}
			break
		}

		// 2. 起始符匹配，不匹配的话，将起始第一字节写入无效数据buf，并移动一个字节。
		sf := segment[0:2]
		if !bytes.Equal(sf, startFlag) {
			if maybeFakeFlag {
				residueBytes = append(residueBytes, segment[0])
			} else {
				invalidBuf.WriteByte(segment[0])
			}
			segment = segment[1:]
			continue
		}

		payloadLen := int(binary.BigEndian.Uint16(segment[pkg.LengthPosition : pkg.LengthPosition+2]))
		length := payloadLen + pkg.PackageLengthWithoutPayload

		if len(segment) < length {
			maybeFakeFlag = true
			residueBytes = append(residueBytes, segment[0])
			segment = segment[1:]
			continue
		}

		err := pkg.VerifyVINascii(segment[3:20])
		if err != nil {
			invalidBuf.WriteByte(segment[0])
			segment = segment[1:]
			continue
		}

		// 4.切出指定长度数据, 并检测。
		waitCheck := segment[:length]
		err = checkBCC(waitCheck)
		if err != nil {
			invalidBuf.WriteByte(segment[0])
			segment = segment[1:]
			continue
		}

		// 检测通过，切分出数据。
		messages = append(messages, waitCheck)
		segment = segment[length:]
		// 检测到正确数据，则缓存的数据一定为假数据，清空
		maybeFakeFlag = false
		residueBytes = residueBytes[:0]
		inbs := invalidBuf.Bytes()
		if len(inbs) > 0 {
			invalidMessages = append(invalidMessages, inbs)
			invalidBuf.Reset()
		}
	}
	residueBytes = append(residueBytes, segment...)
	return messages, residueBytes, invalidMessages
}

func Split32960(segment []byte) (messages [][]byte, residueBytes []byte, invalidMessages [][]byte) {
	var invalidBuf bytes.Buffer

	maybeFakeFlag := false

	for len(segment) > 0 {
		// 1.检查最小长度， 如果长度过短，跳出循环，并清空无效数据缓冲buf。
		if len(segment) < pkg.MinimumPackageLength {
			inbs := invalidBuf.Bytes()
			if len(inbs) > 0 {
				invalidMessages = append(invalidMessages, inbs)
			}
			break
		}

		// 2. 起始符匹配，不匹配的话，将起始第一字节写入无效数据buf，并移动一个字节。
		sf := segment[0:2]
		if !bytes.Equal(sf, gb17691StartFlag) {
			if maybeFakeFlag {
				residueBytes = append(residueBytes, segment[0])
			} else {
				invalidBuf.WriteByte(segment[0])
			}
			segment = segment[1:]
			continue
		}

		// 获取长度。
		length := int(binary.BigEndian.Uint16(segment[pkg.LengthPosition : pkg.LengthPosition+2]))
		length = length + pkg.PackageLengthWithoutPayload // 加上验证码以及头部

		if len(segment) < length {
			maybeFakeFlag = true
			residueBytes = append(residueBytes, segment[0])
			segment = segment[1:]
			continue
		}

		// 4.切出指定长度数据, 并检测。
		waitCheck := segment[:length]
		err := pkg.CheckBCCV1(waitCheck)
		if err != nil {
			invalidBuf.WriteByte(segment[0])
			segment = segment[1:]
			continue
		}

		// 检测通过，切分出数据。
		messages = append(messages, waitCheck)
		segment = segment[length:]
		// 检测到正确数据，则缓存的数据一定为假数据，清空
		maybeFakeFlag = false
		residueBytes = residueBytes[:0]
		inbs := invalidBuf.Bytes()
		if len(inbs) > 0 {
			invalidMessages = append(invalidMessages, inbs)
			invalidBuf.Reset()
		}
	}
	residueBytes = append(residueBytes, segment...)
	return messages, residueBytes, invalidMessages
}

// 808切分
func Split808(segment []byte) (messages [][]byte, residueBytes []byte, invalidMessages [][]byte) {
	//segment = []byte{0x7e, 0x02}
	//segment = []byte{0x02, 0x03, 0x05, 0x06, 0x07}
	//segment = []byte{0x02, 0x03, 0x05, 0x06, 0x7e, 0x02}
	//segment = []byte{0x7e, 0x02, 0x02, 0x03, 0x05, 0x06, 0x07}
	//segment = []byte{0x7e, 0x02, 0x02, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02}
	//segment = []byte{0x7e, 0x02, 0x02, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x7e, 0x02, 0x02, 0x7e, 0x02}
	//segment = []byte{0x7e, 0x02, 0x02, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x7e, 0x02, 0x02, 0x7e, 0x02, 0x7e, 0x02, 0x03, 0x05}
	//segment = []byte{0x7e, 0x02, 0x02, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x77, 0x7e, 0x02, 0x02, 0x7e, 0x02, 0x7e, 0x02, 0x03, 0x05}
	//segment = []byte{0x7e, 0x02, 0x02, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x77, 0x7e, 0x02, 0x02, 0x7e, 0x02}
	//segment = []byte{0x7e, 0x02, 0x02, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x77, 0x7e, 0x02, 0x02, 0x7e, 0x02, 0x77}

	//segment = []byte{0xff, 0xff, 0x7e, 0x02, 0x02, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x77, 0x7e, 0x02, 0x02, 0x7e, 0x02, 0x77}
	//segment = []byte{0xff, 0xff, 0x7e, 0x02, 0x02, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x77, 0x7e, 0x02, 0x7e, 0x02, 0x77}
	//segment = []byte{0x7e, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x77}
	//segment = []byte{0x7e, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x77, 0x7e, 0x02}
	//segment = []byte{0x7e, 0x02, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x77}
	//segment = []byte{0x7e, 0x03, 0x05, 0x06, 0x07, 0x7e, 0x02, 0x77, 0x7e, 0x02, 0x77}
	//segment = []byte{0x77, 0x01, 0x7e, 0x02,   0x7e, 0x02, 0x02, 0x03, 0x05, 0x06, 0x07}
	if len(segment) < 12 {
		residueBytes = append(residueBytes, segment...)
		return
	}
	jtt808StartFlag := 0x7e
	var indexList []int
	for index := 0; index < len(segment); index++ {
		sf := int(segment[index])
		if sf == jtt808StartFlag {
			// 7e list
			indexList = append(indexList, index)
		}
	}
	messages, residueBytes, invalidMessages = SplitPackage(segment, indexList)
	return
}

func SplitPackage(segment []byte, indexList []int) (messages [][]byte, residueBytes []byte, invalidMessages [][]byte) {
	old7e := []byte{0x7d, 0x02}
	source7e := []byte{0x7e}

	old7d := []byte{0x7d, 0x01}
	source7d := []byte{0x7d}
	var entireList []int

	if len(indexList) == 0 {
		residueBytes = append(residueBytes, segment...)
		return
	} else if len(indexList) == 1 {
		left := indexList[0]
		residueBytes = append(residueBytes, segment[left:]...) // 尾
		data := segment[:left]
		invalidMessages = append(invalidMessages, data)
		return
	}

	if len(indexList)%2 != 0 {
		// 有剩余
		entireList = indexList[:len(indexList)-1]
		tail := indexList[len(indexList)-1]
		residueBytes = append(residueBytes, segment[tail:]...) // 尾
		// 7e --- 7e xx 7e 04 03  中间的xx为无效数据
		data := segment[entireList[len(entireList)-1]+1 : tail]
		invalidMessages = append(invalidMessages, data)
	} else {
		// 包完整
		entireList = indexList
		tail := indexList[len(indexList)-1]
		tail += 1
		if tail == len(segment) { // 末尾为0x7e
			residueBytes = append(residueBytes, segment[tail:]...) // 尾
		} else {
			// 中间存在0x7e, 剩下的数据为异常数据
			invalidMessages = append(invalidMessages, segment[tail:])
		}
	}
	head := segment[:indexList[0]]
	invalidMessages = append(invalidMessages, head) // 头

	for index := 0; index < len(entireList)-1; index++ {
		if index%2 == 0 {
			// 起始 -- 结束 中间数据为完整数据包
			data := segment[entireList[index] : entireList[index+1]+1]
			log.Info().Msgf("data is: %x", data)
			pkg := bytes.Replace(data, old7e, source7e, -1)
			pkg = bytes.Replace(pkg, old7d, source7d, -1)
			messages = append(messages, pkg)
		} else {
			// 结束 -- 起始  中间的数据写入无效buffer
			data := segment[entireList[index]+1 : entireList[index+1]]
			if data != nil {
				invalidMessages = append(invalidMessages, data)
			}
		}
	}
	return
}

var oldBytes1 = []byte{0x7D, 0x02}
var newBytes1 = []byte{0x7E}
var oldBytes2 = []byte{0x7D, 0x01}
var newBytes2 = []byte{0x7D}

/*
passList --正确的数据
invalidList -- 无效的数据
remain -- 剩余数据
1. 提取2个0x7e之间的数据
2. 转义还原
3. 校验校验码
4. 解析(消息头固定为10个字节)
*/
func Split808New(bs []byte) (passList [][]byte, invalidList [][]byte, remain []byte) {
	var start bool
	var startIndex, stopIndex int

	size := len(bs)
	dataList := make([][]byte, 0, 4)
	// 1. 提取2个0x7e之间的数据
	for i, bt := range bs {
		if bt == 0x7e {
			if !start {
				startIndex = i
				start = true
			} else {
				stopIndex = i
				start = false
				dataList = append(dataList, bs[startIndex+1:stopIndex]) // 不含标识符
			}
		}
	}
	// 半包的情况
	if start && stopIndex < size {
		remain = bs[startIndex:size]
	}

	// 2. 转义还原
	passList = make([][]byte, 0, len(dataList))
	invalidList = make([][]byte, 0, len(dataList))
	for _, data := range dataList {
		newData := bytes.ReplaceAll(data, oldBytes1, newBytes1)
		newData = bytes.ReplaceAll(newData, oldBytes2, newBytes2)
		// 3. 检查校验码
		if pkg.BCC(newData) == 0 {
			passList = append(passList, newData)
		} else {
			log.Info().Msgf("BCC err, data:%x", newData)
			invalidList = append(invalidList, newData)
		}
	}
	return
}
