package pkg

// Standard constants
const (
	VINLength                   = 17
	MinimumPackageLength        = 25
	PackageLengthWithoutPayload = 25
	LengthPosition              = 22

	StartFlag = "##"

	CommandFlagRealtime        = "\x02"
	CommandFlagPlatformLogin   = "\x05"
	CommandFlagPlatformLogout  = "\x06"
	CommandFlagHeartbeat       = "\x07"
	CommandFlagTimeCalibration = "\x08"

	CommandFlagCommand = "\x80"

	AnswerFlagSuccess = "\x01"
	AnswerFlagFail    = "\x02"
	AnswerFlagCommand = "\xFE"

	EncryptFlagNotEncrypt = "\x01"

	PlaceholderVIN = "00000000000000000"

	ProtocolIM1001 = "im1001"
)
