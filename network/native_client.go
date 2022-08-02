package network

import (
	"bufio"
	"net"
	"time"

	"github.com/rs/zerolog/log"
)

// ClientConfig involve client's configurations
type ClientConfig struct {
	ServAddr      string
	AutoReconnect bool
}

// Client represent a client
type Client struct {
	Config *ClientConfig

	conn               *net.TCPConn
	onConnectionMade   func(c *Client)
	onConnectionClosed func(c *Client, err error)
	onNewMessage       func(c *Client, message []byte)

	connected bool
	started   bool

	ID           string
	ResidueBytes []byte
}

// Send send massage to peer
func (client *Client) Send(message []byte) error {
	if !client.connected {
		return nil
	}

	_, err := client.conn.Write([]byte(message))

	if err != nil {
		log.Error().Str("id", client.ID).Msgf("Error: %v", err.Error())
		return err
	}

	log.Debug().Str("id", client.ID).Msgf("[%v] send: %x", client, message)
	return nil
}

func (client *Client) listen() {
	reader := bufio.NewReader(client.conn)
	buffer := make([]byte, 4096, 4096)
	var read int
	var err error
	for {
		read, err = reader.Read(buffer)

		if err != nil {
			log.Error().Msg(err.Error())
			client.connected = false
			client.onConnectionClosed(client, err)

			if client.Config.AutoReconnect {
				go client.reconnectServer()
			}

			if client.conn != nil {
				client.conn.Close()
			}
			return
		}

		// go transfer byte slice argument as point, so we copy
		// bytes here to prevent race conditions
		segment := make([]byte, read)
		copy(segment, buffer)

		// It turns out that all the goroutines with a CLOSE_WAIT connection will
		// hang on here
		client.onNewMessage(client, segment)
	}
}

func (client *Client) String() string {
	return client.conn.LocalAddr().String()
}

// RemoteAddr return peer's address
func (client *Client) RemoteAddr() net.Addr {
	return client.conn.RemoteAddr()
}

// OnConnectionMade called right after connection with server has been made
func (client *Client) OnConnectionMade(callback func(c *Client)) {
	client.onConnectionMade = callback
}

// OnConnectionClosed called right after connection is closed
func (client *Client) OnConnectionClosed(callback func(c *Client, err error)) {
	client.onConnectionClosed = callback
}

// OnMessageRecived called when Client receives new message
func (client *Client) OnMessageRecived(callback func(c *Client, message []byte)) {
	client.onNewMessage = callback
}

// ConnectServer connect to remote server
func (client *Client) ConnectServer() (err error) {
	client.started = true

	err = client.connectServer()
	if err != nil && client.Config.AutoReconnect {
		go client.reconnectServer()
		return nil
	}
	return err
}

func (client *Client) connectServer() (err error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", client.Config.ServAddr)
	if err != nil {
		log.Error().Str("id", client.ID).Err(err).Msg("")
		return err
	}

	client.conn, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Error().Str("id", client.ID).Err(err).Msg("")
		return err
	}

	client.connected = true
	client.onConnectionMade(client)

	go client.listen()

	return nil
}

func (client *Client) reconnectServer() (err error) {
	for !client.connected && client.started {
		log.Debug().Str("id", client.ID).Msgf("try reconnection...")
		time.Sleep(2 * time.Second)
		client.connectServer()
	}
	return nil
}

// CloseConnect close the connection to peer
// it will not stop client from running if AutoReconnect is set up
// if you want the the client stop, try client.Stop instead
func (client *Client) CloseConnect() (err error) {
	if !client.connected {
		return client.conn.Close()
	}
	return nil
}

// Stop the client from running
func (client *Client) Stop() (err error) {
	client.started = false
	return client.CloseConnect()
}

// NewClient creates new tcp client instance
func NewClient(config *ClientConfig) *Client {
	log.Debug().Msgf("creating client with server address %s", config.ServAddr)
	client := &Client{
		Config: config,
	}

	client.connected = false

	client.OnConnectionMade(func(c *Client) {})
	client.OnMessageRecived(func(c *Client, message []byte) {})
	client.OnConnectionClosed(func(c *Client, err error) {})

	return client
}
