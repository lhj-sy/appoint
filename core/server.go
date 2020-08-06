package core

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

// 注册信号
func HandelSignal(done chan<- struct{})  {
	chan_sigs := make(chan os.Signal, 1)
	signal.Notify(chan_sigs, syscall.SIGQUIT)

	go func() {
		sig := <-chan_sigs

		if sig != nil {
			log.Printf("received a signal %v, close done channel", sig)
			close(done)
		}
	}()
}