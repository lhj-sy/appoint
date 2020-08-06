package main

import (
	"flag"
	"github.com/facebookgo/pidfile"
	"log"
	"msgevent/core"
	"msgevent/util"
	"os"
	"runtime"
)

const (
	ChannelBufferLength = 100 // chan 缓冲区大小
	ReceiverNum         = 5   // 接收者数量
	AckerNum            = 10  // 执行者数量
	ResenderNum         = 5   // 重试者数量

	HttpMaxIdleConns        = 500 // http 最多链接数
	HttpMaxIdleConnsPerHost = 500 // http 最大链接端口数
	HttpIdleConnTimeout     = 30  // http timeout时间
)

var configFileName string
var logFileName string

func cmd() {
	configFileName := flag.String("c", "config/queues.yml", "config file path")
	logFileName := flag.String("log", "", "logging file, default STDOUT")
}

func main() {
	configFileName := flag.String("c", "config/queues.yml", "config file path")
	logFileName := flag.String("log", "", "logging file, default STDOUT")
	flag.Parse()

	pidfile.Write()

	// 设置日志
	if *logFileName != "" {
		f, err := os.OpenFile(*logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		util.PanicOnError(err)
		defer f.Close()

		log.SetOutput(f)
	}

	// 初始化配置
	var allQueues []*core.QueueConfig
	allQueues = core.LoadQueuesConfig(*configFileName, allQueues)

	// 初始化amqp
	_, channel, err := core.AmqpInit()
	if err != nil {
		util.PanicOnError(err)
	}

	for _, queue := range allQueues {
		log.Printf("allQueues: queue config: %v", queue)
		queue.DeclareExchange(channel);
		queue.DeclareQueue(channel)
	}

	// 注册信号
	done := make(chan struct{}, 1)
	core.HandelSignal(done);

	log.Printf("set gorouting to the number of logical CPU: %d", runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	core.SendMessage(allQueues, done) // 发送消息

	log.Printf("exiting program")
}
