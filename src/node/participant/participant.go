package participant

import (
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
}

type Held struct {
	name    string
	currId  int32
	holding bool
	cond    *sync.Cond
	lock    *sync.Mutex
}

var self Participant
var wg sync.WaitGroup

func Start(hostname string, id int) error {
	log.Println("Starting participant")
	self = New(hostname, id)
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

func New(addr string, id int) Participant {
	objs := make(map[string]*Object, 0)
	trans := make(map[int32]*Transaction, 0)
	return Participant{objs, trans, addr, id, make(map[string]*Held, 0)}
}

func NewHeld(name string, currId int32) *Held {
	m := &sync.Mutex{}
	return &Held{name, currId, true, sync.NewCond(m), m}
}
