package participant

import (
	"distributed-transactions/src/rvp"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type Participant struct {
	Objects      map[string]*Object
	Transactions map[int32]*Transaction
	Address      string
	Id           int
	held         map[string]*Held
	monitor      *rvp.Monitor
}

type Held struct {
	name    string
	currId  int32
	holding bool
	cond    *sync.Cond
	lock    *sync.Mutex
}

// This is the real participant instance. The one in setupRPC and the receiver p in RPC methods is an empty instance.
var self Participant
var wg sync.WaitGroup

func Start(hostname string, id int, quitting chan bool) error {
	log.Println("Starting participant")
	self = New(hostname, id, quitting)
	go self.setupRPC()
	wg.Add(1)
	wg.Wait()
	return nil
}

func (p Participant) setupRPC() {
	part := new(Participant)
	rpc.Register(part)
	l, e := net.Listen("tcp", ":"+os.Getenv("RPC_PORT"))
	if e != nil {
		log.Println("Error in setup RPC:", e)
	}
	go rpc.Accept(l)
}

func New(addr string, id int, quitting chan bool) Participant {
	objs := make(map[string]*Object, 0)
	trans := make(map[int32]*Transaction, 0)
	monitor := rvp.NewMonitor(map[string]map[string]bool{"C": {"c": true}})
	go func() {
		_ = <-quitting
		monitor.PrintLog()
		quitting <- true
	}()
	return Participant{objs, trans, addr, id, make(map[string]*Held, 0), monitor}
}

func NewHeld(name string, currId int32) *Held {
	m := &sync.Mutex{}
	return &Held{name, currId, true, sync.NewCond(m), m}
}
