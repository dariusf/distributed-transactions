package participant

import (
	"distributed-transactions/src/rvp"
	"fmt"
	"log"
	"sync"
)

var mutex = &sync.Mutex{}

type CanCommitArgs struct {
	Tid int32
}

type DoCommitArgs struct {
	Tid int32
}

type DoAbortArgs struct {
	Tid int32
}

type JoinArgs struct{}

type SetArgs struct {
	Tid   int32
	Key   string
	Value string
}

type GetArgs struct {
	Tid int32
	Key string
}

type BeginArgs struct{}

func (p *Participant) Join(ja *JoinArgs, reply *Participant) error {
	*reply = self
	return nil
}

func (p *Participant) Begin(ba *BeginArgs, reply *bool) error {
	*reply = true
	log.Println("Initialized all objects for transaction")
	return nil
}

func (p *Participant) CanCommit(cca *CanCommitArgs, reply *bool) error {
	log.Println(self.Transactions, cca.Tid)
	if value, ok := self.Transactions[cca.Tid]; ok {
		if err := self.monitor.StepA(rvp.PReceivePrepare1, "c"); err != nil {
			log.Printf("%v\n", err)
		}
		result := !value.hasFailed()
		*reply = result
		if result {
			if err := self.monitor.StepA(rvp.PSendPrepared2, "c"); err != nil {
				log.Printf("%v\n", err)
			}
		} else {
			if err := self.monitor.StepA(rvp.PSendAbort3, "c"); err != nil {
				log.Printf("%v\n", err)
			}
		}
		return nil
	}
	return fmt.Errorf("No such transaction in server")
}

func (p *Participant) DoCommit(dca *DoCommitArgs, reply *bool) error {
	if value, ok := self.Transactions[dca.Tid]; ok {
		if err := self.monitor.StepA(rvp.PReceiveCommit4, "c"); err != nil {
			log.Printf("%v\n", err)
		}
		for k, v := range self.Transactions[dca.Tid].updates {
			if _, ok := self.Objects[k]; ok {
				self.Objects[k].copyObject(v)
			} else {
				self.Objects[k] = &v
			}
		}
		for k := range self.Objects {
			self.Objects[k].stop()
			self.held[k].holding = false
			self.held[k].currId = 0
			self.held[k].cond.Broadcast()
		}
		value.commit()
		if err := self.monitor.StepA(rvp.PSendCommitAck5, "c"); err != nil {
			log.Printf("%v\n", err)
		}
		*reply = true
		return nil
	}
	return fmt.Errorf("No such transaction in server")
}

func (p *Participant) DoAbort(daa *DoAbortArgs, reply *bool) error {
	if trans, ok := self.Transactions[daa.Tid]; ok {
		if err := self.monitor.StepA(rvp.PReceiveAbort6, "c"); err != nil {
			log.Printf("%v\n", err)
		}
		for k := range self.Objects {
			self.Objects[k].stop()
			self.Objects[k].resetKey(trans.initial[k].Value, daa.Tid)
			self.held[k].holding = false
			self.held[k].cond.Broadcast()
		}
		trans.abort()

		if err := self.monitor.StepA(rvp.PSendAbortAck7, "c"); err != nil {
			log.Printf("%v\n", err)
		}
		*reply = true
		return nil
	}
	return fmt.Errorf("No such transaction in server")
}

func (p *Participant) SetKey(sa *SetArgs, reply *bool) error {
	mutex.Lock()
	defer mutex.Unlock()

	if _, ok := self.Transactions[sa.Tid]; !ok {

		// we need to start a new transaction
		self.Transactions[sa.Tid] = NewTransaction(sa.Tid)

		// set initial state of transaction
		for k := range self.Objects {
			self.Transactions[sa.Tid].addObject(k, *self.Objects[k])
		}
	}

	if _, ok := self.Transactions[sa.Tid].updates[sa.Key]; ok {
		self.Transactions[sa.Tid].updateObject(sa.Key, sa.Value)
		self.held[sa.Key] = NewHeld(sa.Key, sa.Tid)

	} else if obj, ok := self.Objects[sa.Key]; ok {
		self.Transactions[sa.Tid].updates[sa.Key] = *obj
		self.Transactions[sa.Tid].updateObject(sa.Key, sa.Value)
		self.held[sa.Key] = NewHeld(sa.Key, sa.Tid)

	} else {
		obj := NewObject(sa.Key, sa.Value, sa.Tid)
		self.Transactions[sa.Tid].updates[sa.Key] = *obj
		self.held[sa.Key] = NewHeld(sa.Key, sa.Tid)
	}

	*reply = true
	log.Printf("Finished setting %v = %v\n", sa.Key, sa.Value)
	return nil
}

func (p *Participant) GetKey(ga *GetArgs, reply *string) error {
	mutex.Lock()
	defer mutex.Unlock()

	if _, ok := self.Transactions[ga.Tid]; !ok {

		// we need to start a new transaction
		self.Transactions[ga.Tid] = NewTransaction(ga.Tid)

		// set initial state of transaction
		for k, v := range self.Objects {
			if !v.running {
				self.Transactions[ga.Tid].addObject(k, *self.Objects[k])
			}
		}
	}

	if v, ok := self.Transactions[ga.Tid].updates[ga.Key]; ok {
		if v.currTrans != ga.Tid && v.currTrans != 0 {
			*reply = self.Transactions[ga.Tid].initial[ga.Key].Value
			return nil
		} else if _, ok2 := self.Transactions[ga.Tid].updates[ga.Key]; !ok2 {
			*reply = self.Transactions[ga.Tid].initial[ga.Key].Value
			return nil
		}
		o := self.Transactions[ga.Tid].updates[ga.Key]
		*reply = (&o).getKey(ga.Tid)

	} else if v, ok := self.Objects[ga.Key]; ok {
		if v.currTrans != ga.Tid && v.currTrans != 0 {
			*reply = self.Transactions[ga.Tid].initial[ga.Key].Value
			return nil
		} else if _, ok2 := self.Transactions[ga.Tid].updates[ga.Key]; !ok2 {
			*reply = v.getKey(ga.Tid)
			return nil
		}
		o := self.Transactions[ga.Tid].updates[ga.Key]
		*reply = (&o).getKey(ga.Tid)

	} else {
		*reply = "NOT FOUND"
		return fmt.Errorf("No such object in server")
	}

	return nil
}
