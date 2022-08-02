package pkg

// Package is general interface for package
type Package interface {
	CommandFlag() byte
	UniqueCode() []byte
	Payload() []byte
}

// GB32960Package for protocol gb32960
type GB32960Package struct {
	start         []byte
	commandFlag   byte
	answerFlag    byte
	uniqueCode    []byte
	encryptMethod byte
	payloadLength int
	payload       []byte
	checkSum      byte

	rawData []byte
}

// CommandFlag of gb32960 package
func (p GB32960Package) CommandFlag() byte {
	return p.rawData[2]
}

// UniqueCode of gb32960 package
func (p GB32960Package) UniqueCode() []byte {
	return p.rawData[4:21]
}

// Payload of gb32960 package
func (p GB32960Package) Payload() []byte {
	return p.rawData[24 : len(p.rawData)-1]
}

// GB17691Package for protocol gb32960
type GB17691Package struct {
	rawData []byte
}

// CommandFlag of gb17691 package
func (p GB17691Package) CommandFlag() byte {
	return p.rawData[2]
}

// UniqueCode of gb17691 package
func (p GB17691Package) UniqueCode() []byte {
	return p.rawData[3:20]
}

// Payload of gb17691 package
func (p GB17691Package) Payload() []byte {
	return p.rawData[24 : len(p.rawData)-1]
}

// HJPackage for protocol hj
type HJPackage struct {
	rawData []byte
}

// CommandFlag of gb17691 package
func (p HJPackage) CommandFlag() byte {
	return p.rawData[2]
}

// UniqueCode of gb17691 package
func (p HJPackage) UniqueCode() []byte {
	return p.rawData[3:20]
}

// Payload of gb17691 package
func (p HJPackage) Payload() []byte {
	return p.rawData[24 : len(p.rawData)-1]
}

// DeconstractPackage parse byte slice package to package struct
func DeconstractPackage(message []byte, protocol string) (p Package, err error) {
	switch protocol {
	case ProtocolGB32960:
		p = GB32960Package{
			rawData: message,
		}
	case ProtocolGB17691:
		p = GB17691Package{
			rawData: message,
		}
	case ProtocolHJ:
		p = HJPackage{
			rawData: message,
		}
	default:
		p = GB17691Package{
			rawData: message,
		}
	}
	return
}
