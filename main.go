package main

import (
	"distributed-transactions/src/dtrans"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	sigs := make(chan os.Signal, 1)
	quitting := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		_ = <-sigs
		quitting <- true
		_ = <-quitting
		os.Exit(0)
	}()
	dtrans.Start(quitting)
	sigs <- syscall.SIGTERM
	select {}
}

// TODO HANDLE DEADLOCK

// TODO load testing & deadlock testing
