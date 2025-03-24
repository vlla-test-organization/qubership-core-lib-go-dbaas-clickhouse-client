package model

import (
	"github.com/ClickHouse/clickhouse-go/v2"
)

// ClickhouseOptions is a struct which should be used for datasource configuration
type ClickhouseOptions struct {
	Options *clickhouse.Options
}
