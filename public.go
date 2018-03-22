package balancercontrol

import (
	"context"
	"fmt"
	"github.com/temoto/senderbender/alive"
)

type Event struct {
	Command string `json:"command"`
	Host    string `json:"host"`
	Value   string `json:"value"`

	aws eventDataAws
}

type BalancerControl struct {
	alive *alive.Alive
	aws   apiAws
	inch  chan *Event
}

func New() *BalancerControl {
	return &BalancerControl{
		inch: make(chan *Event, 0), // TODO rational decision on buffer size
	}
}

func (self *BalancerControl) Run(ctx context.Context) {
	self.alive.Add(1) // prevent alive finished state until cleanup is done
	stopch := self.alive.StopChan()

runLoop:
	for {
		select {
		case e := <-self.inch:
			go self.controlEvent(ctx, e)
		case <-stopch:
			break runLoop
		}
	}

	// TODO cleanup
	self.alive.Done()
}

func (self *BalancerControl) Send(ctx context.Context, e *Event) error {
	switch e.Command {
	case "upstream-add", "upstream-remove":
		if e.Host == "" {
			return fmt.Errorf("balancercontrol: invalid Event command=%s requires non-empty host", e.Command)
		}
		if e.Value == "" {
			return fmt.Errorf("balancercontrol: invalid Event command=%s requires non-empty value", e.Command)
		}
	case "upstream-force-rebuild-all", "config-reload", "log-reopen":
	default:
		return fmt.Errorf("balancercontrol: invalid Event command: %s", e.Command)
	}

	// TODO: choose transport
	return self.awsSend(ctx, e)
}

func (self *BalancerControl) Stop() { self.alive.Stop() }
func (self *BalancerControl) Wait() { self.alive.Wait() }
