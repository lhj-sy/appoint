package protocols

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// HTTP CLIENT
func HttpClient(maxIdleConns, maxIdleConnsPerHost, idleConnTimeout int) *http.Client {
	tr := &http.Transport{
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     time.Duration(idleConnTimeout) * time.Second,
	}

	client := &http.Client{
		Transport: tr,
	}

	return client
}

// 消息请求
func NotifyUrl(client *http.Client, url string, body []byte) int {
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		log.Printf("notify url create req fail: %s", err)
		return 0
	}

	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	
	if err != nil {
		log.Printf("notify url %s fail: %s", url, err)
		return 0
	}
	defer response.Body.Close()
	
	io.Copy(ioutil.Discard, response.Body) // 丢弃读取完毕的数据 （为了链接复用，需要将上次读取的内容清空）

	return response.StatusCode
}