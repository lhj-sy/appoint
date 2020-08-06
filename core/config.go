package core

import (
	"fmt"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"msgevent/util"
	"strings"
)

// 配置
type Config struct {
	AmqpConfig AmqpConfig `yaml:"amqp"`
}

// AMQP配置
type AmqpConfig struct {
	Uri  string   `yaml:"uri"`
	Host string   `yaml:"host"`
	Port int      `yaml:"port"`
	User string   `yaml:"user"`
	Pwd  string   `yaml:"pwd"`
}

type ProjectsConfig struct {
	Projects []ProjectConfig `yaml:"projects"`
}

type ProjectConfig struct {
	Name     string         `yaml:"name"`
	Default  DefaultConfig  `yaml:"default"`
	Queues   []QueueConfig  `yaml:"queues"`
}

// worker默认配置
type DefaultConfig struct {
	Uri             string 	 `yaml:"uri"`
	Timeout         int 	 `yaml:"timeout"`
	RetryNum      int    	 `yaml:"retry_num"`
	RetryDuration   int    	 `yaml:"retry_duration"`
	BindingExchange string 	 `yaml:"binding_exchange"`
}

// Worker配置
type QueueConfig struct {
	QueueName  		string   `yaml:"queue_name"`
	NotifyPath 		string   `yaml:"notify_path"`
	RoutingKey 		[]string `yaml:"routing_key"`
	Uri             string 	 `yaml:"uri"`
	Timeout         int 	 `yaml:"timeout"`
	RetryNum      int    	 `yaml:"retry_num"`
	RetryDuration   int    	 `yaml:"retry_duration"`
	BindingExchange string 	 `yaml:"binding_exchange"`

	project *ProjectConfig
}

// 队列名称
func (qc QueueConfig) WorkerQueueName() string {
	return qc.QueueName
}

// 重试队列名
func (qc QueueConfig) RetryQueueName() string {
	return fmt.Sprintf("%s-retry", qc.QueueName)
}

// 错误队列名
func (qc QueueConfig) ErrorQueueName() string {
	return fmt.Sprintf("%s-error", qc.QueueName)
}

// 重试交换机名
func (qc QueueConfig) RetryExchangeName() string {
	return fmt.Sprintf("%s-retry", qc.QueueName)
}

// 重入交换机名
func (qc QueueConfig) RequeueExchangeName() string {
	return fmt.Sprintf("%s-retry-requeue", qc.QueueName)
}

// 错误交换机名
func (qc QueueConfig) ErrorExchangeName() string {
	return fmt.Sprintf("%s-error", qc.QueueName)
}

// 工作交换机
func (qc QueueConfig) WorkerExchangeName() string {
	if qc.BindingExchange == "" {
		return qc.project.Default.BindingExchange
	}

	return qc.BindingExchange
}

// 回调地址处理
func (qc QueueConfig) NotifyUrl() string {
	if strings.HasPrefix(qc.NotifyPath, "http://") || strings.HasPrefix(qc.NotifyPath, "https://") {
		return qc.NotifyPath
	}

	return fmt.Sprintf("%s%s", qc.project.Default.Uri, qc.NotifyPath)
}

// 回调超时时间
func (qc QueueConfig) NotifyTimeout() int {
	if qc.Timeout == 0 {
		return qc.project.Default.Timeout
	}

	return qc.Timeout
}

// 重试时间
func (qc QueueConfig) getRetryNum() int {
	if qc.RetryNum == 0 {
		return qc.project.Default.RetryNum
	}

	return qc.RetryNum
}

// 重试间隔
func (qc QueueConfig) getRetryDuration() int {
	if qc.RetryDuration == 0 {
		return qc.project.Default.RetryDuration
	}

	return qc.RetryDuration
}

// AMQP链接地址
func AmqpURL(configPath string) string {
	configFile, err := ioutil.ReadFile(configPath)
	util.PanicOnError(err)

	config := Config{}
	err = yaml.Unmarshal(configFile, &config)
	util.PanicOnError(err)
	amqpConfig := config.AmqpConfig

	return fmt.Sprintf("%s%s:%s@%s:%d", amqpConfig.Uri, amqpConfig.User, amqpConfig.Pwd, amqpConfig.Host, amqpConfig.Port)
}

// 加载配置
func LoadQueuesConfig(configFileName string, allQueues []*QueueConfig) []*QueueConfig {
	configFile, err := ioutil.ReadFile(configFileName)
	util.PanicOnError(err)

	projectsConfig := ProjectsConfig{}
	err = yaml.Unmarshal(configFile, &projectsConfig)
	util.PanicOnError(err)
	log.Printf("find config: %v", projectsConfig)

	projects := projectsConfig.Projects
	for i, project := range projects {
		log.Printf("find project: %s", project.Name)

		queues := projects[i].Queues
		for j, queue := range queues {
			log.Printf("find queue: %v", queue)

			queues[j].project = &projects[j]
			allQueues = append(allQueues, &queues[j])
		}
	}

	return allQueues
}