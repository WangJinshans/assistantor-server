package gateway

import (
	"github.com/BurnishCN/gateway-go/pkg"
)

// QuickResponse generate response for certain types of packages
// to reduce response latency
func QuickResponse(p pkg.Package) (response []byte) {
	c := pkg.NewCreator(string(p.UniqueCode()))
	switch string(p.CommandFlag()) {
	case pkg.CommandFlagHeartbeat:
		return c.ResponseHeartbeat(p.Payload()[:6])
	case pkg.CommandFlagTimeCalibration:
		return c.ResponseTimeCalibration()
	default:
		return
	}
}
