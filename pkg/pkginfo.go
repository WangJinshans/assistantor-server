package pkg

// Package is general interface for package
type Package interface {
	CommandFlag() byte
	UniqueCode() []byte
	Payload() []byte
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


// DeconstractPackage parse byte slice package to package struct
func DeconstractPackage(message []byte, protocol string) (p Package, err error) {
	switch protocol {
	default:
		p = GB17691Package{
			rawData: message,
		}
	}
	return
}
