package main

import (
	"distributed-transactions/src/dtrans"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
)

func main1() {
	dtrans.Start()
}

func main() {
	f, err := os.Create(fmt.Sprintf("profile%s.prof", os.Getenv("NODE_ID")))
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close() // error handling omitted for example
	runtime.SetCPUProfileRate(100000)
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			pprof.StopCPUProfile()
			os.Exit(0)
		}
	}()

	main1()
}

// TODO HANDLE DEADLOCK

// TODO load testing & deadlock testing
