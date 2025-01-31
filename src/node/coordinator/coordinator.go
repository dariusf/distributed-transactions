package coordinator

import (
	"distributed-transactions/src/node/participant"
	"distributed-transactions/src/rvc"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

var host string = "localhost"
var mutex = &sync.Mutex{}
var self Coordinator
var graph *Graph

type Coordinator struct {
	Participants map[string]participant.Participant
	mParts       map[string]bool
	monitor      *rvc.Monitor
}

func Start(quitting chan bool) error {
	log.Println("Starting coordinator..")
	self = New(quitting)

	// set up RPCs
	e := self.setupRPC()
	if e != nil {
		return e
	}

	// join up with participant servers
	total, _ := strconv.Atoi(os.Getenv("NUM_NODES"))
	for i := 2; i < 2+total; i++ {
		go self.joinParticipant(i)
	}

	// set up deadlock detection graph
	graph = NewGraph()

	// interface with client
	return nil
}

func New(quitting chan bool) Coordinator {
	parts := make(map[string]participant.Participant, 0)
	mParts := map[string]bool{}
	monitor := rvc.NewMonitor(map[string]map[string]bool{"P": mParts})
	go func() {
		_ = <-quitting
		monitor.PrintLog()
		quitting <- true
	}()
	c := Coordinator{Participants: parts, mParts: mParts, monitor: monitor}
	return c
}

func (c Coordinator) setupRPC() error {
	e1 := rpc.Register(c)
	if e1 != nil {
		log.Println("Error in register RPC:", e1)
		return e1
	}
	l, e := net.Listen("tcp", ":"+os.Getenv("RPC_PORT"))
	if e != nil {
		log.Println("Error in setup RPC:", e)
		return e
	}
	go rpc.Accept(l)
	return nil
}

func (c Coordinator) joinParticipant(id int) {
	serverId := string(rune('A' + (id - 2)))
	log.Printf("Trying to join node %v\n", serverId)
	hostname := fmt.Sprintf("localhost:%d", 3000+id)

	for {
		client, err := rpc.Dial("tcp", hostname)
		if err != nil {
			continue

		} else {
			var reply participant.Participant
			ja := participant.JoinArgs{}
			err = client.Call("Participant.Join", &ja, &reply)

			if err != nil {
				log.Println("Error in join: ", err)

			} else {
				mutex.Lock()
				c.Participants[serverId] = reply
				c.mParts[serverId] = true
				mutex.Unlock()
				log.Printf("Server %v joined the system\n", serverId)
			}

			// graph.AddVertex(serverId)
			graph.AddVertex(strconv.Itoa(id))
			client.Close()
			return
		}
	}
}
