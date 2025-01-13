// Package internal 避免循环引用
package bcontext

import (
	"context"
	"runtime"
	"strings"
)

// CtxKey 使用自定义类型，不用 string 避免与其他包的 context key 冲突
type CtxKey string

var ctxKeyList []CtxKey

// SetCtxKeyList 设置 context key list, 通过 ValuesFromContext 返回 ctx 里面的 values, package log 里面会用到
func SetCtxKeyList(l []CtxKey) {
	ctxKeyList = l
}

// GetCaller 返回调用的函数信息, eg: common/Func
// The argument skip is the number of stack frames to ascend,
// with 0 identifying the caller of GetCaller()
func GetCaller(skip int) (filePath string, line int, funcName string) {
	skip++
	pc, filePath, line, ok := runtime.Caller(skip)

	details := runtime.FuncForPC(pc)
	if ok && details != nil {
		funcName = details.Name() // eg: real-time-metrics-builder/pkg/Func

		split := strings.Split(funcName, "/")
		if len(split) > 2 {
			funcName = strings.Join(split[2:], ".") // eg: common/Func
		}
	}

	return filePath, line, funcName
}

// GetCallerFuncName 返回调用的函数名
// The argument skip is the number of stack frames to ascend,
// with 0 identifying the caller of GetCallerFuncName()
func GetCallerFuncName(skip int) string {
	skip++
	_, _, funcName := GetCaller(skip)

	return funcName
}

// GetCallerFilePath 返回调用的函数的 filePath
// The argument skip is the number of stack frames to ascend,
// with 0 identifying the caller of GetCallerFilePath()
func GetCallerFilePath(skip int) string {
	skip++
	filePath, _, _ := GetCaller(skip)

	return filePath
}

func ValuesFromContext(ctx context.Context) (keys []string, valueMap map[string]string) {
	valueMap = make(map[string]string)

	for _, k := range ctxKeyList {
		if v, ok := ctx.Value(k).(string); ok {
			keys = append(keys, string(k))
			valueMap[string(k)] = v
		}
	}

	return keys, valueMap
}
