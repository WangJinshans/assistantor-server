package pkg

type IMPackage interface {
	CommandFlag() byte
	UniqueCode() []byte
	Payload() []byte
}

type IMessagePackage struct {
	rawData []byte
}

func (p IMessagePackage) CommandFlag() byte {
	return p.rawData[4]
}

func (p IMessagePackage) UniqueCode() []byte {
	return p.rawData[5:22]
}

func (p IMessagePackage) Payload() []byte {
	return p.rawData[26 : len(p.rawData)-3]
}

func DecodePackage(message []byte, protocol string) (p IMPackage, err error) {
	switch protocol {
	default:
		p = IMessagePackage{
			rawData: message,
		}
	}
	return
}
