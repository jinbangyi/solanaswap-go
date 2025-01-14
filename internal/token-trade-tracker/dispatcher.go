package tokentradetracker

import (
	"fmt"

	"github.com/jinbangyi/solanaswap-go/pkg/log"
	"go.uber.org/zap"
)

/**
- read history trade fomr datetime, auto convert datetime to slot
- write history trade to kafka
*/

// type DispatcherI interface {
// 	// bind tracker and handler
// 	Run() error
// 	// start new tracker and bind to handler
// 	Start() error
// }

type Pipeline struct {
	tracker *TrackerI
	handler *HandlerI
}

type Dispatcher struct {
	// key = handler.name
	tracker map[string]*TrackerI
	handler map[string]*HandlerI

	// key = handler.name
	pipeline map[string]*Pipeline
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{}
}

func (d *Dispatcher) AddTracker(t TrackerI) {
	// add tracker to dispatcher
	if d.tracker == nil {
		d.tracker = make(map[string]*TrackerI)
	}

	_, ok := d.tracker[t.String()]
	if ok {
		log.Warn("tracker already exists", zap.String("tracker", t.String()))
		return
	}

	d.tracker[t.String()] = &t
}

func (d *Dispatcher) AddHandler(h HandlerI) {
	// add handler to dispatcher
	if d.handler == nil {
		d.handler = make(map[string]*HandlerI)
	}

	_, ok := d.handler[h.String()]
	if ok {
		log.Warn("handler already exists", zap.String("handler", h.String()))
		return
	}

	d.handler[h.String()] = &h
}

func (d *Dispatcher) BindTrackerToHandler(trackerName string, handlerName string) error {
	// bind tracker to handler
	tracker, ok := d.tracker[trackerName]
	if !ok {
		return fmt.Errorf("tracker %s not found", trackerName)
	}

	handler, ok := d.handler[handlerName]
	if !ok {
		return fmt.Errorf("handler %s not found", handlerName)
	}

	if d.pipeline == nil {
		d.pipeline = make(map[string]*Pipeline)
	}

	d.pipeline[trackerName] = &Pipeline{
		tracker: tracker,
		handler: handler,
	}

	return nil
}

func (d *Dispatcher) AddPipeline(t TrackerI, h HandlerI) error {
	// TODO when tracker end, close and remove tracker
	d.AddHandler(h)
	d.AddTracker(t)
	err := d.BindTrackerToHandler(t.String(), h.String())
	if err != nil {
		return err
	}

	// start pipeline
	tradeChannel, err := t.ReadTrade()
	if err != nil {
		return err
	}

	return h.WriteTrade(tradeChannel)
}

func (d *Dispatcher) Start() {
	// read history trade
	// write history trade to kafka
}

func (d *Dispatcher) Stop() {
	// stop read history trade
	// stop write history trade to kafka
}

