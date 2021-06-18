package coordinator

import (
	"distributed-transactions/src/node/participant"
	"distributed-transactions/src/rv"
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"time"
)

type BeginArgs struct{}

type SetArgs struct {
	Tid      int32
	MyId     string
	ServerId string
	Key      string
	Value    string
}

type GetArgs struct {
	Tid      int32
	MyId     string
	ServerId string
	Key      string
}

type AbortArgs struct {
	Tid int32
}

type CommitArgs struct {
	Tid int32
}

func (c Coordinator) Begin(ba *BeginArgs, reply *int32) error {
	for _, s := range self.Participants {
		participantId := s.Id
		client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", "localhost", 3000+participantId))
		if err != nil {
			log.Println("Error in Begin/Dial:", err)
			return err
		}

		pba := participant.BeginArgs{}
		var r bool
		err = client.Call("Participant.Begin", &pba, &r)
		if err != nil {
			log.Println("Error in Begin/RPC:", err)
			return err
		}
		client.Close()
	}
	*reply = int32(time.Now().Unix())
	return nil
}

func (c Coordinator) Set(sa *SetArgs, reply *bool) error {
	otherId := fmt.Sprint([]rune(sa.ServerId)[0] - 63)
	id, _ := strconv.Atoi(otherId)

	if _, ok := self.Participants[sa.ServerId]; ok {
		client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", "localhost", 3000+id))
		defer client.Close()

		if err != nil {
			log.Println("Error in Set/Dial: ", err)
			return err
		}

		// add new edge to Graph
		graph.AddVertex(sa.MyId)
		graph.AddEdge(sa.MyId, otherId, sa.Tid)

		// if cycle in Graph caused by this transaction
		if graph.DetectCycle(sa.MyId, otherId) {
			graph.RemoveEdge(sa.MyId, otherId)

			// abort this Transaction
			aa := AbortArgs{sa.Tid}
			var r bool
			c.Abort(&aa, &r)
			return fmt.Errorf("Transaction caused deadlock, aborted")
		}

		// otherwise continue
		psa := participant.SetArgs{sa.Tid, sa.Key, sa.Value}
		err = client.Call("Participant.SetKey", &psa, &reply)
		if err != nil {
			log.Println("Error in Set/RPC: ", err)
			return err
		}

		return nil

	} else {
		return fmt.Errorf("No such server in system\n")
	}
}

func (c Coordinator) Get(ga *GetArgs, reply *string) error {
	otherId := fmt.Sprint([]rune(ga.ServerId)[0] - 63)

	if p, ok := self.Participants[ga.ServerId]; ok {
		client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", "localhost", 3000+p.Id))
		defer client.Close()

		if err != nil {
			log.Println("Error in Get/Dial: ", err)
			return err
		}

		// add new edge to Graph
		graph.AddVertex(ga.MyId)
		graph.AddEdge(ga.MyId, otherId, ga.Tid)

		// if cycle in Graph caused by this transaction
		if graph.DetectCycle(ga.MyId, otherId) {
			graph.RemoveEdge(ga.MyId, otherId)

			// abort this Transaction
			aa := AbortArgs{ga.Tid}
			var r bool
			c.Abort(&aa, &r)
			return fmt.Errorf("Transaction caused deadlock, aborted")
		}

		// abort transaction
		pga := participant.GetArgs{ga.Tid, ga.Key}
		err = client.Call("Participant.GetKey", &pga, &reply)
		if err != nil {
			return err
		}

		return nil

	} else {
		return fmt.Errorf("No such server in system\n")
	}
}

func (c Coordinator) Commit(ca *CommitArgs, reply *bool) error {
	defer graph.RemoveTransaction(ca.Tid)

	// check if we can commit
	cca := participant.CanCommitArgs{ca.Tid}
	for pid, p := range self.Participants {
		if err := c.monitor.StepA(rv.CSendPrepare8, pid); err != nil {
			log.Printf("%v\n", err)
		}
		client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", "localhost", 3000+p.Id))
		if err != nil {
			log.Println("Error in Commit/Dial:", err)
			client.Close()
			return err
		}

		var check bool
		err = client.Call("Participant.CanCommit", &cca, &check)
		if err != nil {
			if err.Error() != "No such transaction in server" {
				log.Println("Error in Commit/RPC:", err)
				client.Close()
				return err
			} else {
				continue
			}
		}

		if !check {
			*reply = false
			log.Println("Someone said no!")
			if err := c.monitor.StepA(rv.CReceiveAbort10, pid); err != nil {
				log.Printf("%v\n", err)
			}
			client.Close()
			return nil
		}
		if err := c.monitor.StepA(rv.CReceivePrepared9, pid); err != nil {
			log.Printf("%v\n", err)
		}
		client.Close()
	}

	// if we can, we commit
	dca := participant.DoCommitArgs{ca.Tid}
	for pid, p := range self.Participants {
		client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", "localhost", 3000+p.Id))
		if err != nil {
			log.Println("Error in DoCommit/Dial:", err)
			client.Close()
			return err
		}

		var check bool
		err = client.Call("Participant.DoCommit", &dca, &check)
		if err := c.monitor.StepA(rv.CSendCommit11, pid); err != nil {
			log.Printf("%v\n", err)
		}
		if err != nil && err.Error() != "No such transaction in server" {
			log.Println("Error in DoCommit/RPC:", err)
			client.Close()
			return err
		}
		if err := c.monitor.StepA(rv.CReceiveCommitAck12, pid); err != nil {
			log.Printf("%v\n", err)
		}
		client.Close()
	}

	c.monitor.PrintLog()

	*reply = true
	return nil
}

func (c Coordinator) Abort(aa *AbortArgs, reply *bool) error {
	defer graph.RemoveTransaction(aa.Tid)

	paa := participant.DoAbortArgs{aa.Tid}
	for pid, p := range self.Participants {
		client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", "localhost", 3000+p.Id))
		if err != nil {
			log.Println("Error in Abort/Dial:", err)
			return err
		}

		var check bool
		err = client.Call("Participant.DoAbort", &paa, &check)
		if err := c.monitor.StepA(rv.CSendAbort13, pid); err != nil {
			log.Printf("%v\n", err)
		}
		if err != nil && err.Error() != "No such transaction in server" {
			log.Println("Error in DoAbort/RPC:", err)
			client.Close()
			return err
		}
		if err := c.monitor.StepA(rv.CReceiveAbortAck14, pid); err != nil {
			log.Printf("%v\n", err)
		}

		client.Close()
	}

	return nil
}
