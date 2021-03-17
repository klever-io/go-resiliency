// Package breaker implements the circuit-breaker resiliency pattern for Go.
package breaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrBreakerOpen is the error returned from Run() when the function is not executed
// because the breaker is currently open.
var ErrBreakerOpen = errors.New("circuit breaker is open")

const (
	Closed uint32 = iota
	Open
	HalfOpen
)

// Breaker implements the circuit-breaker resiliency pattern
type Breaker struct {
	ErrorThreshold, SuccessThreshold int
	Timeout                          time.Duration

	Lock              sync.Mutex
	State             uint32
	Errors, Successes int
	LastError         time.Time
}

// New constructs a new circuit-breaker that starts closed.
// From closed, the breaker opens if "ErrorThreshold" Errors are seen
// without an error-free period of at least "Timeout". From open, the
// breaker half-closes after "Timeout". From half-open, the breaker closes
// after "SuccessThreshold" consecutive successes, or opens on a single error.
func New(ErrorThreshold, SuccessThreshold int, Timeout time.Duration) *Breaker {
	return &Breaker{
		ErrorThreshold:   ErrorThreshold,
		SuccessThreshold: SuccessThreshold,
		Timeout:          Timeout,
	}
}

// Run will either return ErrBreakerOpen immediately if the circuit-breaker is
// already open, or it will run the given function and pass along its return
// value. It is safe to call Run concurrently on the same Breaker.
func (b *Breaker) Run(work func() error) error {
	state := atomic.LoadUint32(&b.State)

	if state == Open {
		return ErrBreakerOpen
	}

	return b.doWork(state, work)
}

// GetState will return the current circuit breaker State in order inform how it is
// based on the initialization of this class
func (b *Breaker) GetState() string {
	switch b.State {
	case 0:
		return "Closed"
	case 1:
		return "Open"
	case 2:
		return "HalfOpen"
	default:
		return ""
	}
}

// Go will either return ErrBreakerOpen immediately if the circuit-breaker is
// already open, or it will run the given function in a separate goroutine.
// If the function is run, Go will return nil immediately, and will *not* return
// the return value of the function. It is safe to call Go concurrently on the
// same Breaker.
func (b *Breaker) Go(work func() error) error {
	State := atomic.LoadUint32(&b.State)

	if State == Open {
		return ErrBreakerOpen
	}

	// errcheck complains about ignoring the error return value, but
	// that's on purpose; if you want an error from a goroutine you have to
	// get it over a channel or something
	go b.doWork(State, work)

	return nil
}

func (b *Breaker) doWork(State uint32, work func() error) error {
	var panicValue interface{}

	result := func() error {
		defer func() {
			panicValue = recover()
		}()
		return work()
	}()

	if result == nil && panicValue == nil && State == Closed {
		// short-circuit the normal, success path without contending
		// on the Lock
		return nil
	}

	// oh well, I guess we have to contend on the Lock
	b.processResult(result, panicValue)

	if panicValue != nil {
		// as close as Go lets us come to a "rethrow" although unfortunately
		// we lose the original panicing location
		panic(panicValue)
	}

	return result
}

func (b *Breaker) processResult(result error, panicValue interface{}) {
	b.Lock.Lock()
	defer b.Lock.Unlock()

	if result == nil && panicValue == nil {
		if b.State == HalfOpen {
			b.Successes++
			if b.Successes == b.SuccessThreshold {
				b.closeBreaker()
			}
		}
	} else {
		if b.Errors > 0 {
			expiry := b.LastError.Add(b.Timeout)
			if time.Now().After(expiry) {
				b.Errors = 0
			}
		}

		switch b.State {
		case Closed:
			b.Errors++
			if b.Errors == b.ErrorThreshold {
				b.openBreaker()
			} else {
				b.LastError = time.Now()
			}
		case HalfOpen:
			b.openBreaker()
		}
	}
}

func (b *Breaker) openBreaker() {
	b.changeState(Open)
	go b.timer()
}

func (b *Breaker) closeBreaker() {
	b.changeState(Closed)
}

func (b *Breaker) timer() {
	time.Sleep(b.Timeout)

	b.Lock.Lock()
	defer b.Lock.Unlock()

	b.changeState(HalfOpen)
}

func (b *Breaker) changeState(newState uint32) {
	b.Errors = 0
	b.Successes = 0
	atomic.StoreUint32(&b.State, newState)
}
