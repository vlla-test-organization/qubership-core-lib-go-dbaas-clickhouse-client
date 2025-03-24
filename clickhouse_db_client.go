package clickhousedbaas

import (
	"context"

	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/cache"
	"github.com/netcracker/qubership-core-lib-go-dbaas-clickhouse-client/v1/model"
)

var logger logging.Logger

func init() {
	logger = logging.GetLogger("clickhousedbaas")
}

const (
	propMicroserviceName = "microservice.name"
)

func NewClient(pool *dbaasbase.DbaaSPool) *DbaaSClickhouseClient {
	localCache := cache.DbaaSCache{
		LogicalDbCache: make(map[cache.Key]interface{}),
	}
	return &DbaaSClickhouseClient{
		chClientCache: localCache,
		pool:          pool,
	}
}

type DbaaSClickhouseClient struct {
	chClientCache cache.DbaaSCache
	pool          *dbaasbase.DbaaSPool
}

func (d *DbaaSClickhouseClient) ServiceDatabase(params ...model.DbParams) Database {
	return &database{
		params:          d.buildServiceDbParams(params),
		dbaasPool:       d.pool,
		clickhouseCache: &d.chClientCache,
	}
}

func (d *DbaaSClickhouseClient) buildServiceDbParams(params []model.DbParams) model.DbParams {
	localParams := model.DbParams{}
	if params != nil {
		localParams = params[0]
	}
	if localParams.Classifier == nil {
		localParams.Classifier = ServiceClassifier
	}
	return localParams
}

func (d *DbaaSClickhouseClient) TenantDatabase(params ...model.DbParams) Database {
	return &database{
		params:          d.buildTenantDbParams(params),
		dbaasPool:       d.pool,
		clickhouseCache: &d.chClientCache,
	}
}

func (d *DbaaSClickhouseClient) buildTenantDbParams(params []model.DbParams) model.DbParams {
	localParams := model.DbParams{}
	if params != nil {
		localParams = params[0]
	}
	if localParams.Classifier == nil {
		localParams.Classifier = TenantClassifier
	}
	return localParams
}

func ServiceClassifier(ctx context.Context) map[string]interface{} {
	return dbaasbase.BaseServiceClassifier(ctx)
}

func TenantClassifier(ctx context.Context) map[string]interface{} {
	return dbaasbase.BaseTenantClassifier(ctx)
}
