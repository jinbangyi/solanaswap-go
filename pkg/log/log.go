/*
Package log 对 zap 的 logger 进行了封装，默认根据 env 环境通过 NewDefault() 初始化 logger
如果要将 log 写入文件，需要在程序结束时执行 log.Sync() 确保所有 log 写入磁盘
可以通过 RestoreDefault() 更改 default logger, 但可能不保证线程安全，尽量不这样做

Usage:

	import "log"

	// 对于正式的 log 使用 zap.Logger
	log.Info("this is a msg", zap.String("app", app))
	log.Warn("this is a msg", zap.String("app", app))
	log.WrapError("this is a msg", zap.String("app", app))

	// 对于非正式的 log 使用语法糖 zap.SugarLogger
	log.S.Debug("this is a msg ", "another msg ", a, b)
	log.S.Debugw("this is a msg", "objectA", objectA, "objectB", objectB)

	// 程序结束时
	log.Sync()
*/
package log

import (
	"context"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"github.com/jinbangyi/solanaswap-go/pkg/config"
	bcontext "github.com/jinbangyi/solanaswap-go/pkg/context"
)

var S = logger.Sugar() // Zap SugarLogger

var (
	Info   = logger.Info
	Warn   = logger.Warn
	Error  = logger.Error
	DPanic = logger.DPanic
	Panic  = logger.Panic
	Fatal  = logger.Fatal
	Debug  = logger.Debug
	Sync   = logger.Sync

	// enhanced log
	ErrorWithContext = errorWithContext
)

type Logger = zap.Logger

var (
	lock   sync.Mutex
	logger = NewDefault()
)

func Default() *zap.Logger {
	return logger
}

func NewDefault() *zap.Logger {
	env := config.GetEnv()

	l, err := New(env)
	if err != nil {
		panic(fmt.Errorf("log new logger for env:%s %w", env, err))
	}

	return l
}

// New create a new logger (not support log rotating).
func New(env config.Env) (*zap.Logger, error) {
	var (
		l   *zap.Logger
		err error
	)

	switch env {
	case config.Local:
		if l, err = zap.NewDevelopment(); err != nil {
			return nil, err
		}

	case config.Dev:
		if l, err = zap.NewDevelopment(); err != nil {
			return nil, err
		}

		l = l.WithOptions(zap.IncreaseLevel(zap.InfoLevel))

	default:
		if l, err = zap.NewProduction(); err != nil {
			return nil, err
		}
	}

	return l, nil
}

// ResetDefault 重设默认的 logger， may not be safe for concurrent use
func ResetDefault(l *zap.Logger) {
	lock.Lock()
	defer lock.Unlock()

	logger = l
	S = logger.Sugar()

	Info = logger.Info
	Warn = logger.Warn
	Error = logger.Error
	DPanic = logger.DPanic
	Panic = logger.Panic
	Fatal = logger.Fatal
	Debug = logger.Debug
	Sync = logger.Sync
}

var (
	// warningNumCounter warning 警告数量统计
	warningNumCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "basev1",
		Name:      "warning_num_total",
		Help:      "warning 的总次数",
	}, []string{"group", "name"})

	// warningRateSetter warning 警告阈值设置
	warningRateSetter = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "basev1",
		Name:      "warning_rate",
		Help:      "warning 报警阈值，10 秒内 warning 次数超过该阈值才报警",
	}, []string{"group", "name"})
)

// AlertOpt 警告参数设置
type AlertOpt struct {
	Group string  // 报警分组, 可选, 不填默认是 ""
	Name  string  // 报警名称, 可选，不填默认是 msg[:20]
	Rate  float64 // 报警阈值, 10 秒内次数超过该阈值才报警
}

// WarnAlert 打印 warn 日志并发送报警
// 需要在程序启动时调用 prometheus.Start()
func WarnAlert(opt AlertOpt, msg string, fields ...Field) {
	logger.WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)

	// increment the warning count and set the warning rate
	if opt.Name == "" {
		opt.Name = msg
		if len(opt.Name) > 20 {
			opt.Name = opt.Name[:20]
		}
	}
	labels := prometheus.Labels{
		"group": opt.Group,
		"name":  opt.Name,
	}
	warningRateSetter.With(labels).Set(opt.Rate)
	warningNumCounter.With(labels).Inc()
}

func GetPrometheusWarningNumCounter() *prometheus.CounterVec {
	return warningNumCounter
}

func GetPrometheusWarningRateSetter() *prometheus.GaugeVec {
	return warningRateSetter
}

type Fields []zap.Field

func (f *Fields) Add(fields ...Field) Fields {
	*f = append(*f, fields...)
	return *f
}

func (f Fields) With(fields ...Field) Fields {
	return append(f, fields...)
}

func (f Fields) WithError(err error) Fields {
	return append(f, zap.Error(err))
}

func ContextFields(ctx context.Context, fields ...Field) Fields {
	keys, valueMap := bcontext.ValuesFromContext(ctx)

	ret := make([]zap.Field, 0, len(keys)+len(fields))
	for _, k := range keys {
		ret = append(ret, zap.String("ctx."+k, valueMap[k]))
	}

	ret = append(ret, fields...)

	return ret
}

func errorWithContext(ctx context.Context, key string, msg string, err error) {
	fields := ContextFields(ctx).With(zap.Error(err))
	logger.Error(key+"|"+msg, fields...)
}
