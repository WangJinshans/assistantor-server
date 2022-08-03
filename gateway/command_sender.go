package gateway

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/BurnishCN/gateway-go/command_sender"
	"github.com/BurnishCN/gateway-go/common"
	"github.com/BurnishCN/gateway-go/network"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
)

const (
	// RedisKeyCommandHistory record commands status
	RedisKeyCommandHistory = "commands-history"
	// RedisKeyCommandBacklogQueue record backlogged commands
	RedisKeyCommandBacklogQueue = "commands-backlog"
)

type GrpcServer struct {
	underlyingServer network.Server
	redisClient      *redis.Client
}

// 下行命令
// TODO 移除redis结果更改 放到长链接管理器中
func (s *GrpcServer) SendCommand(ctx context.Context, arg *pb.SendParameter) (*pb.SendResult, error) {
	result := s.sendCommand(arg)
	log.Info().Msgf("grpc result is: %v", result)
	//if result.Success {
	//	commandSendKey := fmt.Sprintf("%s:%s:%d", common.CommandKey, arg.Vin, arg.Timestamp)
	//	dataMap := make(map[string]interface{})
	//	dataMap["status"] = common.CommandSendSuccess
	//	_, err := s.redisClient.HMSet(commandSendKey, dataMap).Result()
	//	if err != nil {
	//		log.Error().Msgf("update status error: %v", err)
	//	}
	//}
	return result, nil
}

// 发送命令
func (s *GrpcServer) sendCommand(arg *pb.SendParameter) (result *pb.SendResult) {
	log.Info().Msgf("receive command from command client: %v", arg)
	var err error
	conn := s.underlyingServer.GetConn(arg.Vin)
	if conn != nil {
		err = conn.Send(arg.Command)
		if err != nil {
			log.Error().Msgf("send back error: %s", err.Error())
			result = &pb.SendResult{
				Success:      false,
				ErrorMessage: err.Error(),
			}
			return
		}
		result = &pb.SendResult{
			Success: true,
		}
	} else {
		err = errors.New(common.EmptyConnection)
		result = &pb.SendResult{
			Success:      false,
			ErrorMessage: err.Error(),
		}
	}
	return
}

// StartCommandServer start a command port on gateway process to receive downstream command
func StartCommandServer(port int, underlyingServer network.Server, redisClient *redis.Client) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	pb.RegisterSendServer(s, &GrpcServer{
		underlyingServer: underlyingServer,
		redisClient:      redisClient,
	})

	reflection.Register(s)
	log.Info().Msgf("command sender server at %v", addr)
	err = s.Serve(lis)
	if err != nil {
		log.Error().Msgf("start grpc server error: %v", err.Error())
		panic(err)
	}
}

// StartEpollCommandServer start a command port on gateway process to receive downstream command with epoll server
func StartEpollCommandServer(port int, underlyingServer *EpollServer, redisClient *redis.Client) error {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Info().Msgf("command sender server at %v", port)
	s := grpc.NewServer()
	pb.RegisterSendServer(s, &GrpcEpollServer{
		server:      underlyingServer,
		redisClient: redisClient,
	})

	reflection.Register(s)
	return s.Serve(lis)
}

type GrpcEpollServer struct {
	server      *EpollServer
	redisClient *redis.Client
}

// 下行命令
func (s *GrpcEpollServer) SendCommand(ctx context.Context, arg *pb.SendParameter) (*pb.SendResult, error) {
	err := s.sendCommand(arg)
	if err != nil {
		return &pb.SendResult{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}
	return &pb.SendResult{Success: true}, nil
}

// 发送命令
func (s *GrpcEpollServer) sendCommand(arg *pb.SendParameter) (err error) {
	log.Info().Msgf("receive command from command client: %s", arg)
	conn := s.server.GetConn(arg.Vin)
	if conn != nil {
		_, err = conn.conn.Write(arg.Command)
		if err != nil {
			log.Error().Msg(err.Error())
			return
		}
	} else {
		err = errors.New(common.EmptyConnection)
	}
	return
}
