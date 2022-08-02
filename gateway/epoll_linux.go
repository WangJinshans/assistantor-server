// +build linux

package gateway

import (
	"net"
	"reflect"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

type epoll struct {
	fd          int
	connections map[int]*EpollConnection
	lock        *sync.RWMutex
}

func MkEpoll() (*epoll, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &epoll{
		fd:          fd,
		lock:        &sync.RWMutex{},
		connections: make(map[int]*EpollConnection),
	}, nil
}

func (e *epoll) Add(conn *EpollConnection) error {
	// Extract file descriptor associated with the EpollConnection
	fd := socketFD(*conn.conn)
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.POLLIN | unix.POLLHUP, Fd: int32(fd)})
	if err != nil {
		return err
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	e.connections[fd] = conn
	return nil
}

func (e *epoll) Remove(conn *EpollConnection) error {
	fd := socketFD(*conn.conn)
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return err
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.connections, fd)
	return nil
}

func (e *epoll) Wait() ([]*EpollConnection, error) {
	events := make([]unix.EpollEvent, 100)
retry:
	n, err := unix.EpollWait(e.fd, events, -1)
	if err != nil {
		if err == unix.EINTR {
			goto retry
		}
		return nil, err
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	var connections []*EpollConnection
	for i := 0; i < n; i++ {
		conn := e.connections[int(events[i].Fd)]

		// we must check if conn is nil since sometimes it is nil
		// TODO: Find out why it is sometimes nil
		if conn != nil {
			connections = append(connections, conn)
		}
	}

	return connections, nil
}

func socketFD(conn net.TCPConn) int {
	tcpConn := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("conn")
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")

	// conn
	return int(pfdVal.FieldByName("Sysfd").Int())
}
