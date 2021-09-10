package node

import (
	"distributed-transactions/src/node/coordinator"
	"distributed-transactions/src/node/participant"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

var host string = "localhost"
var nodeId int

func Start(quitting chan bool) {
	log.Println("Starting server..")
	hostname := "localhost"
	for i := 1; i < 10; i++ {
		name := fmt.Sprintf(host, i)
		if name == hostname {
			nodeId = i
			break
		}
	}
	nodeId, _ = strconv.Atoi(os.Getenv("NODE_ID"))
	log.Printf("Node ID is %v\n", nodeId)
	if os.Getenv("COORDINATOR") == "1" {
		// if id is 1, is Coordinator
		go coordinator.Start(quitting)
	} else {
		// otherwise participant
		go participant.Start(hostname, nodeId, quitting)
	}
	// handle everything else there
}

func getHostName() string {
	out, err := exec.Command("hostname").Output()
	if err != nil {
		log.Println("Failed to obtain hostname")
		return ""
	}
	return strings.TrimSpace(string(out))
}
