package clickhousedbaas

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/netcracker/qubership-core-lib-go/v3/utils"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/cache"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
	"github.com/netcracker/qubership-core-lib-go-dbaas-clickhouse-client/v1/model"
)

const (
	AuthenticationFailedErrorCode = 516
)

type ClickhouseClient interface {
	GetConnection(ctx context.Context) (driver.Conn, error)
}

// ChClientImpl stores cache of databases and params for databases creation and getting connections
type chClientImpl struct {
	options         *clickhouse.Options
	dbaasClient     dbaasbase.DbaaSClient
	clickhouseCache *cache.DbaaSCache
	params          model.DbParams
}

func (p *chClientImpl) GetConnection(ctx context.Context) (driver.Conn, error) {
	classifier := p.params.Classifier(ctx)
	key := cache.NewKey(DB_TYPE, classifier)
	clickhouseConn, err := p.getOrCreateDb(ctx, key, classifier)
	if err != nil {
		return nil, err
	}
	if pErr := clickhouseConn.Ping(ctx); pErr != nil {
		logger.Warnf("connection ping failed with err: %v. Deleting conn from cache and recreating connection", pErr)
		p.clickhouseCache.Delete(key)
		clickhouseConn, err = p.getOrCreateDb(ctx, key, classifier)
		if err != nil {
			return nil, err
		}
	}
	if valid, vErr := p.isPasswordValid(ctx, clickhouseConn); !valid && vErr == nil {
		logger.Info("authentication error, try to get new password")
		newConnection, dbErr := p.dbaasClient.GetConnection(ctx, DB_TYPE, classifier, p.params.BaseDbParams)
		if dbErr != nil {
			logger.ErrorC(ctx, "Can't update connection with dbaas")
			return nil, dbErr
		}
		err := clickhouseConn.Close()
		if err != nil {
			logger.ErrorC(ctx, "Couldn't disconnect from existing clickhouse connection")
			return nil, err
		}
		clickhouseOpts, err := p.buildClickhouseOptions(newConnection)
		if err != nil {
			return nil, err
		}
		logger.Debug("Build go-clickhouse client for database with classifier %+v and type %s", classifier, DB_TYPE)
		clickhouseConn, err = clickhouse.Open(clickhouseOpts)
		if err != nil {
			logger.Errorf("Error during opening clickhouse connection %+v", err.Error())
			return nil, err
		}

		logger.Info("db password updated successfully")
	} else if vErr != nil {
		logger.Errorf("connection ping failed with err: %v.", vErr.Error())
		return nil, vErr
	}
	return clickhouseConn, nil
}

func (p *chClientImpl) getOrCreateDb(ctx context.Context, key cache.Key, classifier map[string]interface{}) (driver.Conn, error) {
	rawChDb, err := p.clickhouseCache.Cache(key, p.createNewClickhouseDb(ctx, classifier))
	if err != nil {
		return nil, err
	}
	return rawChDb.(driver.Conn), nil
}

func (p *chClientImpl) createNewClickhouseDb(ctx context.Context, classifier map[string]interface{}) func() (interface{}, error) {
	return func() (interface{}, error) {
		logger.Debug("Create clickhouse database with classifier %+v", classifier)
		logicalDb, err := p.dbaasClient.GetOrCreateDb(ctx, DB_TYPE, classifier, p.params.BaseDbParams)
		if err != nil {
			return nil, err
		}
		clickhouseOpts, err := p.buildClickhouseOptions(logicalDb.ConnectionProperties)
		if tls, ok := logicalDb.ConnectionProperties["tls"].(bool); ok && tls {
			logger.Infof("Connection to clickhouse db will be secured")
			clickhouseOpts.TLS = utils.GetTlsConfig()
		}
		if err != nil {
			return nil, err
		}
		logger.Debug("Build go-clickhouse client for database with classifier %+v and type %s", classifier, DB_TYPE)

		clickConn, err := clickhouse.Open(clickhouseOpts)
		if err != nil {
			logger.Errorf("Error during opening clickhouse connection %+v", err.Error())
			return nil, err
		}

		return clickConn, nil
	}
}

func (p *chClientImpl) isPasswordValid(ctx context.Context, conn driver.Conn) (bool, error) {
	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return exception.Code != int32(AuthenticationFailedErrorCode), nil
		}
		return false, err
	}
	return true, nil
}

func (p *chClientImpl) getNewConnectionProperties(ctx context.Context, classifier map[string]interface{}, params rest.BaseDbParams) (*model.ClickhouseConnProperties, error) {
	newConnection, dbErr := p.dbaasClient.GetConnection(ctx, DB_TYPE, classifier, params)
	if dbErr != nil {
		logger.ErrorC(ctx, "Can't update connection with dbaas")
		return nil, dbErr
	}

	connectionProperties := toClickhouseConnProperties(newConnection)
	return &connectionProperties, nil
}

func (p *chClientImpl) buildClickhouseOptions(connProperties map[string]interface{}) (*clickhouse.Options, error) {
	connectionProperties := toClickhouseConnProperties(connProperties)
	connOpts, err := clickhouse.ParseDSN(connectionProperties.Url)
	if err != nil {
		return nil, err
	}
	opts := &clickhouse.Options{}
	if p.options != nil {
		opts = p.options
	}
	opts.Auth = clickhouse.Auth{
		Database: connOpts.Auth.Database,
		Username: connectionProperties.Username,
		Password: connectionProperties.Password,
	}
	opts.Addr = connOpts.Addr
	return opts, nil
}
