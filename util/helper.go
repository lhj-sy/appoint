package util

import "fmt"

// 打印错误日志
func LogOnError(err error) {
	if err != nil {
		fmt.Printf("ERROR - %s\n", err)
	}
}

// panic
func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
