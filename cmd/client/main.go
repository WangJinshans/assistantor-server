package main

import (
	"bufio"
	"encoding/hex"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/BurnishCN/gateway-go/client"
	"github.com/BurnishCN/gateway-go/gateway"
	"github.com/BurnishCN/general-go/consul"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	enableLogLine = true

	// consul
	consulAddr string
)

func init() {
	gateway.ConfigLogFromEnv()

	// env
	env := os.Getenv("ENV")
	app := os.Getenv("APP")
	if env == "" {
		env = "dev"
	}
	if app == "" {
		app = "gateway"
	}
	consul.SetEnv(env)
	consul.SetApp(app)
	consulAddr = os.Getenv("CONSUL_ADDR")
	if consulAddr == "" {
		consulAddr = "127.0.0.1:8300"
	}
}

func main() {
	var err error
	var workload int

	tc := client.TruckConfig{}

	workloadString := os.Getenv("CLIENT_WORKLOAD")
	if workloadString == "" {
		workload = 1
	} else {
		workload, err = strconv.Atoi(workloadString)
		if err != nil {
			panic(err)
		}
	}
	tc.ServeAddr = os.Getenv("CLIENT_SERVER_ADDRESS")
	if tc.ServeAddr == "" {
		tc.ServeAddr = "127.0.0.1:11000"
	}
	sendIntervalString := os.Getenv("CLIENT_SEND_INTERVAL")
	if sendIntervalString == "" {
		tc.SendInterval = 10
	} else {
		tc.SendInterval, err = strconv.ParseFloat(sendIntervalString, 32)
		if err != nil {
			panic(err)
		}
	}
	tc.Protocol = os.Getenv("CLIENT_PROTOCOL")
	if tc.Protocol == "" {
		tc.Protocol = "hj"
	}

	tc.EnableCommandReply = true
	opt := os.Getenv("ENABLE_COMMAND_REPLY")
	if opt == "false" {
		tc.EnableCommandReply = false
	}

	var clientCmd = &cobra.Command{
		Use: "endpoint",
		Run: func(cmd *cobra.Command, args []string) {
			go client.ServePrometheus()

			// go func() {
			// 	time.Sleep(30 * time.Second)
			// 	target := float64(workload) / tc.SendInterval
			// 	log.Info().Msgf("pps target: %f", target)
			// 	client.AdjustInterval(target, time.Duration(int64(tc.SendInterval)) * time.Second)
			// }()
			start(workload, tc)
		},
	}
	flags := clientCmd.Flags()
	flags.IntVarP(&workload, "num", "n", workload, "number of clients")
	flags.Float64VarP(&tc.SendInterval, "interval", "i", tc.SendInterval, "send interval")
	flags.StringVarP(&tc.Vin, "vin", "v", tc.Vin, "vin")
	flags.StringVarP(&tc.ServeAddr, "server-addr", "s", tc.ServeAddr, "server address")
	flags.StringVarP(&tc.Protocol, "protocol", "p", tc.Protocol, "protocol")
	flags.BoolVarP(&tc.IsSign, "sign", "", tc.IsSign, "")
	flags.BoolVarP(&tc.IsAlert, "alert", "a", tc.IsAlert, "send alert package")

	if err := clientCmd.Execute(); err != nil {
		log.Fatal().Msgf("err: %v", err)
	}
}

func start(truckNum int, tc client.TruckConfig) {
	if err := consul.InitConsul(consulAddr); err != nil {
		log.Fatal().Msgf("InitConsul err: %v", err)
	}
	var err error
	var keep int64
	var keepString string
	keepString = os.Getenv("KEEP_STRING")
	if keepString == "" {
		keep = 90
	} else {
		keep, err = strconv.ParseInt(keepString, 10, 0)
		if err != nil {
			panic(err)
		}
	}

	var times int64
	var timeString string
	timeString = os.Getenv("TIME_STRING")
	if timeString == "" {
		times = 600
	} else {
		times, err = strconv.ParseInt(timeString, 10, 0)
		if err != nil {
			panic(err)
		}
	}

	var vins []string
	stopCh := make(chan struct{})
	stoppedSignalCh := make(chan struct{}, truckNum+1)
	verifyDataCh := make(chan []byte, 50000)
	for i := 0; i < truckNum; i++ {
		truck := client.NewTruck(tc)
		vins = append(vins, truck.Vin)
		//go truck.Ignition(verifyDataCh, stopCh, stoppedSignalCh, writeVerifyDataToFile2)
		go truck.Ignition(verifyDataCh, stopCh, stoppedSignalCh, int(keep))
		time.Sleep(1 * time.Millisecond)
	}

	writeVINToFile(vins)
	// 存储抽样数据
	go writeVerifyDataToFile(verifyDataCh, stopCh, stoppedSignalCh)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	timer := time.After(time.Duration(times) * time.Second)
	stop := false
out:
	for {
		select {
		case <-sigCh:
			break out
		case <-timer:
			stop = true
			close(stopCh)
		default:
			if stop && len(stoppedSignalCh) == truckNum+1 {
				time.Sleep(3 * time.Second)
				writeCounterToFile()
				//break out
			}
		}
	}

}

func writeVINToFile(vins []string) {
	var filename string
	name := os.Getenv("POD_NAME")
	if name == "" {
		filename = "/data/vins" + time.Now().String() + ".txt"
	} else {
		filename = "/data/vins" + name + ".txt"
	}

	file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal().Msgf("failed creating file: %s", err)
	}

	datawriter := bufio.NewWriter(file)

	for _, data := range vins {
		_, _ = datawriter.WriteString(data + "\n")
	}

	datawriter.Flush()
	file.Close()
}

func writeVerifyDataToFile(cacheCh chan []byte, stopped chan struct{}, stoppedSignalCh chan struct{}) {
	var filename string
	name := os.Getenv("POD_NAME")
	if name == "" {
		filename = "/data/verify_data" + time.Now().String() + ".txt"
	} else {
		filename = "/data/verify_data" + name + ".txt"
	}
	//file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal().Msgf("failed creating file: %s", err)
	}
	defer file.Close()

	datawriter := bufio.NewWriter(file)
	bufCount := 0
	for {
		select {
		case <-stopped:
			//for data := range cacheCh {
			//	datawriter.WriteString(hex.EncodeToString(data) + "\n")
			//	bufCount++
			//	if bufCount > 200 {
			//		datawriter.Flush()
			//		bufCount = 0
			//	}
			//}
			stoppedSignalCh <- struct{}{}
			return
		case data := <-cacheCh:
			datawriter.WriteString(hex.EncodeToString(data) + "\n")
			bufCount++
			if bufCount > 200 {
				datawriter.Flush()
				bufCount = 0
			}
		}
	}
}

func writeCounterToFile() {
	resp, err := http.Get("http://127.0.0.1:8081/metrics")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	var filename string
	name := os.Getenv("POD_NAME")
	if name == "" {
		filename = "/data/statistics" + time.Now().String() + ".txt"
	} else {
		filename = "/data/statistics" + name + ".txt"
	}
	file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal().Msgf("failed creating file: %s", err)
	}

	datawriter := bufio.NewWriter(file)
	datawriter.WriteString(string(body))
	datawriter.Flush()
	file.Close()
}
