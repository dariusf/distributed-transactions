package rvc

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

type Global struct {
	HasAborted bool
	Committed  map[string]bool
	Aborted    map[string]bool
}

type Action int

const (
	CSendPrepare8 Action = iota
	CReceivePrepared9
	CReceiveAbort10
	CSendCommit11
	CReceiveCommitAck12
	CSendAbort13
	CReceiveAbortAck14
)

func all(s []string, f func(string) bool) bool {
	b := true
	for _, v := range s {
		b = b && f(v)
	}
	return b
}

func allSet(s map[string]bool, f func(string) bool) bool {
	b := true
	for k := range s {
		b = b && f(k)
	}
	return b
}

func any(s []string, f func(string) bool) bool {
	b := false
	for _, v := range s {
		b = b || f(v)
	}
	return b
}

func anySet(s map[string]bool, f func(string) bool) bool {
	b := false
	for k := range s {
		b = b || f(k)
	}
	return b
}

func (m *Monitor) precondition(g *Global, action Action, params ...string) error {
	switch action {
	case CSendPrepare8:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		// no preconditions
		if !(m.PC["Ct0_"+(params[0] /* p : P */)] == 0) {
			return fmt.Errorf("control precondition of CSendPrepare8 %v violated", params)
		}
		m.Log = append(m.Log, entry{action: "CSendPrepare8", params: params})
		return nil
	case CReceivePrepared9:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		// no preconditions
		if !(m.PC["Ct0_"+(params[0] /* p : P */)] == 8) {
			return fmt.Errorf("control precondition of CReceivePrepared9 %v violated", params)
		}
		m.Log = append(m.Log, entry{action: "CReceivePrepared9", params: params})
		return nil
	case CReceiveAbort10:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		// no preconditions
		if !(m.PC["Ct0_"+(params[0] /* p : P */)] == 8) {
			return fmt.Errorf("control precondition of CReceiveAbort10 %v violated", params)
		}
		m.Log = append(m.Log, entry{action: "CReceiveAbort10", params: params})
		return nil
	case CSendCommit11:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		if g != nil && !(!(g.HasAborted)) {
			return fmt.Errorf("logical precondition of %s, %v violated", "CSendCommit11", params)
		}
		if !(allSet(m.vars["P"], func(p string) bool { return m.PC["Ct0_"+(p)] == 9 || m.PC["Ct0_"+(p)] == 10 })) {
			return fmt.Errorf("control precondition of CSendCommit11 %v violated", params)
		}
		m.Log = append(m.Log, entry{action: "CSendCommit11", params: params})
		return nil
	case CReceiveCommitAck12:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		// no preconditions
		if !(m.PC["Ct2_"+(params[0] /* p : P */)] == 11) {
			return fmt.Errorf("control precondition of CReceiveCommitAck12 %v violated", params)
		}
		m.Log = append(m.Log, entry{action: "CReceiveCommitAck12", params: params})
		return nil
	case CSendAbort13:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		if g != nil && !(g.HasAborted) {
			return fmt.Errorf("logical precondition of %s, %v violated", "CSendAbort13", params)
		}
		if !(allSet(m.vars["P"], func(p string) bool { return m.PC["Ct0_"+(p)] == 9 || m.PC["Ct0_"+(p)] == 10 })) {
			return fmt.Errorf("control precondition of CSendAbort13 %v violated", params)
		}
		m.Log = append(m.Log, entry{action: "CSendAbort13", params: params})
		return nil
	case CReceiveAbortAck14:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		// no preconditions
		if !(m.PC["Ct1_"+(params[0] /* p : P */)] == 13) {
			return fmt.Errorf("control precondition of CReceiveAbortAck14 %v violated", params)
		}
		m.Log = append(m.Log, entry{action: "CReceiveAbortAck14", params: params})
		return nil
	default:
		panic("invalid action")
	}
}

func (m *Monitor) applyPostcondition(action Action, params ...string) error {
	switch action {
	case CSendPrepare8:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		m.PC["Ct0_"+(params[0] /* p : P */)] = 8
	case CReceivePrepared9:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		m.PC["Ct0_"+(params[0] /* p : P */)] = 9
	case CReceiveAbort10:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		m.PC["Ct0_"+(params[0] /* p : P */)] = 10
	case CSendCommit11:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		m.PC["Ct2_"+(params[0] /* p : P */)] = 11
	case CReceiveCommitAck12:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		m.PC["Ct2_"+(params[0] /* p : P */)] = 12
	case CSendAbort13:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		m.PC["Ct1_"+(params[0] /* p : P */)] = 13
	case CReceiveAbortAck14:
		if len(params) != 1 {
			return errors.New("expected 1 params")
		}
		m.PC["Ct1_"+(params[0] /* p : P */)] = 14
	default:
		panic("invalid action")
	}
	return nil
}

// LTL property 1

// Propositions
func (l *LTLMonitor1) t0(g Global) bool {
	return reflect.DeepEqual((len(g.Committed) + len(g.Aborted)), len(l.vars["P"]))
}
func (l *LTLMonitor1) t1(g Global) bool {
	return (reflect.DeepEqual(g.Committed, map[string]bool{}) || reflect.DeepEqual(g.Aborted, map[string]bool{}))
}

type State1 int

const (
	S_0_R State1 = iota
	S_1_Y
)

type LTLMonitor1 struct {
	state     State1
	succeeded bool
	failed    bool
	vars      map[string]map[string]bool
}

func NewLTLMonitor1(vars map[string]map[string]bool) *LTLMonitor1 {
	return &LTLMonitor1{
		vars:      vars,
		state:     S_1_Y,
		succeeded: false,
		failed:    false,
	}
}

func (l *LTLMonitor1) StepLTL1(g Global) error {
	if l.succeeded {
		return nil
	} else if l.failed {
		return errors.New("property falsified")
	}

	// evaluate all the props
	t0 := l.t0(g)
	t1 := l.t1(g)

	// note the true ones, take that transition
	switch l.state {
	case S_0_R:
		if t0 {
			if t1 {
				l.state = S_0_R
				l.failed = true
				return errors.New("property falsified")
			} else {
				l.state = S_0_R
				l.failed = true
				return errors.New("property falsified")
			}
		} else {
			if t1 {
				l.state = S_0_R
				l.failed = true
				return errors.New("property falsified")
			} else {
				l.state = S_0_R
				l.failed = true
				return errors.New("property falsified")
			}
		}
	case S_1_Y:
		if t0 {
			if t1 {
				l.state = S_1_Y
				return nil
			} else {
				l.state = S_0_R
				l.failed = true
				return errors.New("property falsified")
			}
		} else {
			if t1 {
				l.state = S_1_Y
				return nil
			} else {
				l.state = S_1_Y
				return nil
			}
		}
	default:
		panic("invalid state")
	}
}

type entry struct {
	action string
	params []string
}

type Log = []entry

type Monitor struct {
	previous Global
	PC       map[string]int
	//vars     map[string][]string
	vars            map[string]map[string]bool
	ltlMonitor1     *LTLMonitor1
	Log             Log
	ExecutionTimeNs int64
	lock            sync.Mutex
}

//func NewMonitor(vars map[string][]string) *Monitor {
func NewMonitor(vars map[string]map[string]bool) *Monitor {
	return &Monitor{
		// previous is the empty Global
		PC:          map[string]int{}, // not the smae as a nil map
		vars:        vars,
		ltlMonitor1: NewLTLMonitor1(vars),
		// Everything else uses mzero
	}
}

func (m *Monitor) Reset() {
	m.lock.Lock()
	defer m.lock.Unlock()
	defer m.trackTime(time.Now())

	m.previous = Global{}
	m.PC = map[string]int{}
	// vars ok
	m.ltlMonitor1 = NewLTLMonitor1(m.vars)
	m.Log = Log{}

	// This is deliberately not reset, to track the total time the monitor has been used
	// m.ExecutionTimeNs = 0

	// lock ok
}

func (m *Monitor) Step(g Global, act Action, params ...string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	defer m.trackTime(time.Now())

	if err := m.precondition(&g, act, params...); err != nil {
		return err
	}

	m.previous = g

	if err := m.applyPostcondition(act, params...); err != nil {
		return err
	}

	// LTL monitors

	if err := m.ltlMonitor1.StepLTL1(g); err != nil {
		return err
	}

	return nil
}

func (m *Monitor) StepA(act Action, params ...string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	defer m.trackTime(time.Now())

	if err := m.precondition(nil, act, params...); err != nil {
		return err
	}

	if err := m.applyPostcondition(act, params...); err != nil {
		return err
	}

	return nil
}

func (m *Monitor) StepS(g Global) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	defer m.trackTime(time.Now())

	m.previous = g

	// LTL monitors

	if err := m.ltlMonitor1.StepLTL1(g); err != nil {
		return err
	}

	return nil
}

func (m *Monitor) PrintLog() {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, e := range m.Log {
		fmt.Printf("%s %v\n", e.action, e.params)
	}
	// fmt.Printf("Monitor time taken: %v\n", time.Duration(m.ExecutionTimeNs))
	fmt.Printf("Monitor time taken: %d\n", m.ExecutionTimeNs)
}

func (m *Monitor) trackTime(start time.Time) {
	elapsed := time.Since(start)
	m.ExecutionTimeNs += elapsed.Nanoseconds()
}
