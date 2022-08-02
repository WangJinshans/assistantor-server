package network

import "time"

// Server is a general purpose interface for tcp server
type Server interface {
	RegisterCallbacks(
		onConnectionMade func(c *Connection, vin string),
		onConnectionClosed func(c *Connection, err error),
		onMessageReceived func(c *Connection, message []byte),
	)

	OnConnectionMade(c *Connection, vin string)
	OnConnectionClosed(c *Connection, err error)
	OnMessageReceived(c *Connection, message []byte)

	SubmitRequest() // 用于计算qps 由于连接被独立,难以计算服务的QPS,若将MessageChan放到server中,将对messageReceive的性能有很高的要求,否则数据切割会成为瓶颈
	// 从connection提出到server中 多个协程一起工作 没办法保证连接中的数据连续性
	//func (s *NaiveServer) dispatchMessage(c *Connection) {
	//	for i:=0;i<20;i++{
	//		go func() {
	//			for {
	//				select {
	//				case message := <-s.MessageChan:
	//					s.OnMessageReceived(c, message)
	//				}
	//			}
	//		}()
	//	}
	//}

	GetTimeout() time.Duration

	// addConn add a id-conn pair to connections
	addConn(id string, conn *Connection)
	// removeConn remove a named connection to connections
	removeConn(id string)

	// GetConn get connection object via id
	GetConn(id string) *Connection
	// GetIDSet get id set (for connection reporting)
	GetIDSet() map[string]bool

	// Listen start server's running loop
	Listen()
	// Stop stop server's running loop
	Stop()
}
