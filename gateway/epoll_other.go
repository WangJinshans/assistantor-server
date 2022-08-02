// +build !linux

package gateway

import (
	"net"
	"sync"
)

type epoll struct {
	fd          int
	connections map[int]*EpollConnection
	lock        *sync.RWMutex
}

func MkEpoll() (*epoll, error) {
	return nil, nil
}

func (e *epoll) Add(conn *EpollConnection) error {
	return nil
}

func (e *epoll) Remove(conn *EpollConnection) error {
	return nil
}

func (e *epoll) Wait() ([]*EpollConnection, error) {
	return nil, nil
}

func socketFD(conn net.TCPConn) int {
	return 0
}