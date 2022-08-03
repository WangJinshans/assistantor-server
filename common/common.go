package common

const (
	CommandQueryServiceName = "query_service" // 连接查询的服务名称
	CommandSendServiceName  = "command_service"
	ConnectionKey           = "connections"
	ConnectionHostKey       = "connection_hosts"
	CommandCacheKey         = "command_cache" // command_cache_vin_timestamp hash expire 3*24  last_update 保存最后更新时间 status 保存状态
	CommandKey              = "down_command_status"
	EmptyConnection         = "empty connection"
	ParameterError          = "extra parameter error" // 附加参数错误
	DefaultError            = "default error"         // 默认错误
)

const (
	CommandSendSuccess = "success"
	CommandSendFailed  = "failed"
)

const (
	PlatFormXev    = "xev"
	PlatFormXevCar = "xev_car" // xev 车端
	PlatFormJmc    = "jmc"     // 江玲
)

const (
	LoginEvent  = 1 // 登录事件
	LogoutEvent = 2 // 登出事件
)
