package task

import (
	"context"
	"sync"
	"time"

	bcontext "github.com/jinbangyi/solanaswap-go/pkg/context"
	"github.com/jinbangyi/solanaswap-go/pkg/log"

	"go.uber.org/zap"
)

const (
	module = "task_monitor"
)

type Task interface {
	fields() log.Fields
	Run()
}

// Runner Task which only run once
type Runner struct {
	ctx  context.Context
	name string
	f    func(context.Context) error
	wg   *sync.WaitGroup
}

// NewRunner returns task that only run once
func NewRunner(ctx context.Context, name string, f func(context.Context) error, wg *sync.WaitGroup) *Runner {
	ctx = bcontext.ContextWithTaskName(ctx, name)

	wg.Add(1)

	return &Runner{
		ctx:  ctx,
		name: name,
		f:    f,
		wg:   wg,
	}
}

func (task *Runner) fields() log.Fields {
	return log.ContextFields(
		task.ctx,
		zap.String("module", module),
	)
}

func (task *Runner) Run() {
	fields := task.fields()

	if task.wg != nil {
		defer task.wg.Done()
	}

	defer func() {
		if err := recover(); err != nil {
			fields.Add(zap.Any("error", err))
			log.DPanic("task panic", fields...)
		}
	}()

	log.Info("task start", fields...)

	st := time.Now()
	err := task.f(task.ctx)

	fields.Add(
		zap.Duration("duration", time.Since(st)),
		zap.Error(err),
	)

	if err != nil {
		log.Warn("task fail", fields...)
		return
	}

	log.Info("task finish", fields...)
}

// MustRunner Task which only run once, and will panic if error
type MustRunner struct {
	ctx  context.Context
	name string
	f    func(context.Context)
	wg   *sync.WaitGroup
}

// NewMustRunner returns task that only run once, use for func that will panic if error
func NewMustRunner(ctx context.Context, name string, f func(context.Context), wg *sync.WaitGroup) *MustRunner {
	ctx = bcontext.ContextWithTaskName(ctx, name)

	wg.Add(1)

	return &MustRunner{
		ctx:  ctx,
		name: name,
		f:    f,
		wg:   wg,
	}
}

func (task *MustRunner) fields() log.Fields {
	return log.ContextFields(
		task.ctx,
		zap.String("module", module),
	)
}

func (task *MustRunner) Run() {
	fields := task.fields()

	if task.wg != nil {
		defer task.wg.Done()
	}

	log.Info("task start", fields...)

	st := time.Now()

	task.f(task.ctx)

	fields.Add(zap.Duration("duration", time.Since(st)))
	log.Info("task finish", fields...)
}

// Daemon Task that should not stop, such as listening to websocket / message queue
// Daemon will auto restart if it stops
type Daemon struct {
	ctx             context.Context
	name            string
	f               func(context.Context) error
	errorRetryAfter time.Duration
	wg              *sync.WaitGroup
}

type DaemonConfig struct {
	ErrorRetryAfter time.Duration
}

// NewDaemon returns task that should not stop and will auto restart if it stops
func NewDaemon(ctx context.Context, name string, f func(context.Context) error,
	wg *sync.WaitGroup, config DaemonConfig,
) *Daemon {
	name = "daemon:" + name
	ctx = bcontext.ContextWithTaskName(ctx, name)

	wg.Add(1)

	return &Daemon{
		ctx:             ctx,
		name:            name,
		f:               f,
		wg:              wg,
		errorRetryAfter: config.ErrorRetryAfter,
	}
}

func (task *Daemon) fields() log.Fields {
	return log.ContextFields(
		task.ctx,
		zap.String("module", module),
		zap.Duration("config.retry_after", task.errorRetryAfter),
	)
}

func (task *Daemon) run() {
	fields := task.fields()

	defer func() {
		if err := recover(); err != nil {
			fields.Add(zap.Any("error", err))
			log.DPanic("task panic", fields...)
		}
	}()

	log.Info("task start", fields...)

	st := time.Now()
	err := task.f(task.ctx)

	fields.Add(zap.Duration("duration", time.Since(st)), zap.Error(err))
	log.Warn("task stop", fields...)
}

func (task *Daemon) Run() {
	fields := task.fields()

	if task.wg != nil {
		defer task.wg.Done()
	}

	for {
		task.run()

		select {
		case <-task.ctx.Done():
			log.Info("task context cancel", fields...)
			return
		case <-time.After(task.errorRetryAfter):
		}
	}
}

// Looper Task that will loop do things and sleep
type Looper struct {
	ctx  context.Context
	name string
	f    func(context.Context) error
	wg   *sync.WaitGroup

	interval   time.Duration
	retryAfter time.Duration
}

type LooperConfig struct {
	Interval   time.Duration
	RetryAfter time.Duration
}

// NewLooper returns task that will loop do things and sleep
func NewLooper(ctx context.Context, name string, f func(context.Context) error,
	wg *sync.WaitGroup, config LooperConfig,
) *Looper {
	name = "loop:" + name
	ctx = bcontext.ContextWithTaskName(ctx, name)

	wg.Add(1)

	return &Looper{
		ctx:  ctx,
		name: name,
		f:    f,
		wg:   wg,

		interval:   config.Interval,
		retryAfter: config.RetryAfter,
	}
}

func (task *Looper) fields() log.Fields {
	return log.ContextFields(
		task.ctx,
		zap.String("module", module),
		zap.Duration("config.interval", task.interval),
		zap.Duration("config.retry_after", task.retryAfter),
	)
}

func (task *Looper) run() error {
	fields := task.fields()

	defer func() {
		if err := recover(); err != nil {
			fields.Add(zap.Any("error", err))
			log.DPanic("task panic", fields...)
		}
	}()

	log.Info("task start", fields...)

	st := time.Now()
	err := task.f(task.ctx)

	fields.Add(
		zap.Duration("duration", time.Since(st)),
		zap.Error(err),
	)

	if err != nil {
		log.Warn("task finish", fields...)
		return err
	}

	log.Info("task finish", fields...)

	return nil
}

func (task *Looper) Run() {
	fields := task.fields()

	if task.wg != nil {
		defer task.wg.Done()
	}

	for {
		for {
			if err := task.run(); err == nil {
				break
			}
			// retry
			select {
			case <-task.ctx.Done():
				log.Info("task context done, exit", fields...)
				return
			case <-time.After(task.retryAfter):
			}
		}

		// sleep
		select {
		case <-task.ctx.Done():
			log.Info("task context done, exit", fields...)
			return

		case <-time.After(task.interval):
		}
	}
}
