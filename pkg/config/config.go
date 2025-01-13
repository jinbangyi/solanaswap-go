/*
Package config 对读取配置的 viper 库进行了封装，从文件和环境变量中读取 config

原始的 viper.Get(key) 当 key 不存在是返回 Nil, 不会报错
原始的 viper.GetInt(key) 当 key 不存在，或者 value 不是 Int 类型时不会报错，返回零值
因此封装了 GetMust 方法，key 不存在时 Panic
封装了 GetIntMust 等方法，当 key 不存在时或值不会相应类型时 Panic

Usage:

	import "config"
	config.GetStringMust("mongo.orderbook.uri")
	config.GetStringMust("mongo", dbName, "uri") // 语法糖 key = "mongo.{dbName}.uri"

	config.GetIntMust("redis.data.db") // 不会 Int 类型时 Panic
*/
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

type Env string

const (
	Local Env = "local"
	Dev   Env = "dev"
	Pre   Env = "pre"
	Prod  Env = "prod"

	EnvKey        = "GO_ENV"
	ConfigPathKey = "CONFIG_PATH"
)

const sep = "_"

// init 使用 viper 读取配置文件和环境变量, 初始化 config
func init() {
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "__"))
	viper.SetDefault(EnvKey, "local")
	viper.SetDefault(ConfigPathKey, "")
	viper.SetEnvPrefix("")
	viper.AutomaticEnv()

	viper.SetConfigType("yaml")

	// 根据 ENV 设置 config 文件
	switch Env(viper.GetString(EnvKey)) {
	case Local:
		viper.SetConfigName("config-local")
	case Dev:
		viper.SetConfigName("config-dev")
	case Pre:
		viper.SetConfigName("config-pre")
	case Prod:
		viper.SetConfigName("config-prod")
	}

	envConfigPath := viper.GetString(ConfigPathKey)
	if envConfigPath != "" {
		viper.AddConfigPath(envConfigPath)
		if err := viper.ReadInConfig(); err != nil {
			panic(fmt.Errorf("fatal error config file: %w", err))
		}
	} else {
		cwd, _ := os.Getwd()
		if err := readInConfig(cwd); err != nil {
			panic(fmt.Errorf("fatal error config file: %w", err))
		}
	}

	b, err := json.Marshal(viper.AllSettings())
	if err != nil {
		panic(err)
	}

	fmt.Println("load config", string(b)) //nolint:forbidigo
}

// readInConfig 读取配置文件
func readInConfig(path string) error {
	if path == "" || len(path) <= 0 {
		return errors.New("error conf path")
	}

	confPath := filepath.Join(path, "/config")
	viper.AddConfigPath(confPath)
	viper.Set(ConfigPathKey, confPath)

	if err := viper.ReadInConfig(); err != nil {
		if strings.EqualFold(path, userHomeDir()) {
			return err
		}

		readInConfig(filepath.Dir(path))
	}

	return nil
}

func userHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}

	return os.Getenv("HOME")
}

func GetMust(keys ...string) interface{} {
	key := strings.Join(keys, sep)

	v := viper.Get(key)
	if v == nil {
		panic("config not found key: " + key)
	}

	return v
}

func GetStringMust(keys ...string) string {
	key := strings.Join(keys, sep)

	v, err := cast.ToStringE(GetMust(key))
	if err != nil {
		panic("config value cast to string error: " + key)
	}

	return v
}

func GetStringSliceMust(keys ...string) []string {
	key := strings.Join(keys, sep)

	v, err := cast.ToStringSliceE(GetMust(key))
	if err != nil {
		panic("config value cast to string slice error: " + key)
	}

	return v
}

func GetIntMust(keys ...string) int {
	key := strings.Join(keys, sep)

	v, err := cast.ToIntE(GetMust(key))
	if err != nil {
		panic("config value cast to int error: " + key)
	}

	return v
}

func GetDurationMust(keys ...string) time.Duration {
	key := strings.Join(keys, sep)

	v, err := cast.ToDurationE(GetMust(key))
	if err != nil {
		panic("config value cast to duration error: " + key)
	}

	return v
}

func GetEnv() Env {
	return Env(GetStringMust(EnvKey))
}

func IsLocal() bool {
	return GetEnv() == Local
}

func IsDev() bool {
	return GetEnv() == Dev
}

func IsProd() bool {
	return GetEnv() == Prod
}
