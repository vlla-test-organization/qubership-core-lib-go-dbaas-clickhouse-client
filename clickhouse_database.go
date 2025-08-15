package clickhousedbaas

import (
	"context"

	dbaasbase "github.com/vlla-test-organization/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/vlla-test-organization/qubership-core-lib-go-dbaas-base-client/v3/cache"
	"github.com/vlla-test-organization/qubership-core-lib-go-dbaas-clickhouse-client/v2/model"
)

const (
	DB_TYPE = "clickhouse"
)

type Database interface {
	GetClickhouseClient(options ...*model.ClickhouseOptions) (ClickhouseClient, error)
	GetConnectionProperties(ctx context.Context) (*model.ClickhouseConnProperties, error)
	FindConnectionProperties(ctx context.Context) (*model.ClickhouseConnProperties, error)
}

type database struct {
	params          model.DbParams
	dbaasPool       *dbaasbase.DbaaSPool
	clickhouseCache *cache.DbaaSCache
}

func (d database) GetClickhouseClient(options ...*model.ClickhouseOptions) (ClickhouseClient, error) {
	clientOptions := &model.ClickhouseOptions{}
	if options != nil {
		clientOptions = options[0]
	}
	return &chClientImpl{
		options:         clientOptions.Options,
		dbaasClient:     d.dbaasPool.Client,
		clickhouseCache: d.clickhouseCache,
		params:          d.params,
	}, nil
}

func (d database) GetConnectionProperties(ctx context.Context) (*model.ClickhouseConnProperties, error) {
	baseDbParams := d.params.BaseDbParams
	classifier := d.params.Classifier(ctx)

	chLogicalDb, err := d.dbaasPool.GetOrCreateDb(ctx, DB_TYPE, classifier, baseDbParams)
	if err != nil {
		logger.Error("Error acquiring connection properties from DBaaS: %v", err)
		return nil, err
	}
	chConnectionProperties := toClickhouseConnProperties(chLogicalDb.ConnectionProperties)
	return &chConnectionProperties, nil
}

func (d database) FindConnectionProperties(ctx context.Context) (*model.ClickhouseConnProperties, error) {
	classifier := d.params.Classifier(ctx)
	params := d.params.BaseDbParams
	responseBody, err := d.dbaasPool.GetConnection(ctx, DB_TYPE, classifier, params)
	if err != nil {
		logger.ErrorC(ctx, "Error finding connection properties from DBaaS: %v", err)
		return nil, err
	}
	logger.Info("Found connection to clickhouse db with classifier %+v", classifier)
	ClickhouseConnProperties := toClickhouseConnProperties(responseBody)
	return &ClickhouseConnProperties, err
}

func toClickhouseConnProperties(connProperties map[string]interface{}) model.ClickhouseConnProperties {
	return model.ClickhouseConnProperties{
		Url:      connProperties["url"].(string),
		Username: connProperties["username"].(string),
		Password: connProperties["password"].(string),
	}
}
