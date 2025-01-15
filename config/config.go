package config

import (
	"strings"

	"github.com/jinbangyi/solanaswap-go/pkg/config"
)

var (
	HTTP_ADDR      = ":8080"

	// PG_BLOCKCHAIN_FETCHER_HOST     = config.GetStringMust("postgresql.blockchain_fetcher.host")
	// PG_BLOCKCHAIN_FETCHER_USER     = config.GetStringMust("postgresql.blockchain_fetcher.username")
	// PG_BLOCKCHAIN_FETCHER_PORT     = config.GetIntMust("postgresql.blockchain_fetcher.port")
	// PG_BLOCKCHAIN_FETCHER_PASSWORD = config.GetStringMust("postgresql.blockchain_fetcher.password")
	// PG_BLOCKCHAIN_FETCHER_DATABASE = config.GetStringMust("postgresql.blockchain_fetcher.db")

	KAFKA_BROKERS           = getStringSliceConfig("kafka.default.brokers")
)

func getStringSliceConfig(key string) []string {
	confValue := config.GetStringSliceMust(key)
	if len(confValue) == 1 {
		confValue = strings.Split(confValue[0], ",")
	}

	return confValue
}
