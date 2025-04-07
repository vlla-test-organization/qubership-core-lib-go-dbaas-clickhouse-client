package clickhousedbaas

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model"
	. "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/testutils"
	chmodel "github.com/netcracker/qubership-core-lib-go-dbaas-clickhouse-client/v2/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	dbaasAgentUrlEnvName = "dbaas.agent"
	namespaceEnvName     = "microservice.namespace"
	testServiceName      = "service_test"
	createDatabaseV3     = "/api/v3/dbaas/test_namespace/databases"
	getDatabaseV3        = "/api/v3/dbaas/test_namespace/databases/get-by-classifier/clickhouse"
	username             = "service_test"
	password             = "qwerty127"
)

type DatabaseTestSuite struct {
	suite.Suite
	database Database
}

func (suite *DatabaseTestSuite) SetupSuite() {
	StartMockServer()
	os.Setenv(dbaasAgentUrlEnvName, GetMockServerUrl())
	os.Setenv(namespaceEnvName, "test_namespace")
	os.Setenv(propMicroserviceName, testServiceName)

	yamlParams := configloader.YamlPropertySourceParams{ConfigFilePath: "testdata/application.yaml"}
	configloader.Init(configloader.BasePropertySources(yamlParams)...)
}

func (suite *DatabaseTestSuite) TearDownSuite() {
	os.Unsetenv(dbaasAgentUrlEnvName)
	os.Unsetenv(namespaceEnvName)
	os.Unsetenv(propMicroserviceName)
	StopMockServer()
}

func (suite *DatabaseTestSuite) BeforeTest(suiteName, testName string) {
	suite.T().Cleanup(ClearHandlers)
	dbaasPool := dbaasbase.NewDbaaSPool()
	client := NewClient(dbaasPool)
	suite.database = client.ServiceDatabase()
}

func (suite *DatabaseTestSuite) TestServiceDbaasChClient_FindDbaaSClickhouseConnection() {
	AddHandler(Contains(getDatabaseV3), defaultDbaasResponseHandler)

	ctx := context.Background()
	actualResponse, err := suite.database.FindConnectionProperties(ctx)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), password, actualResponse.Password)
	assert.Equal(suite.T(), username, actualResponse.Username)
}

func (suite *DatabaseTestSuite) TestServiceDbaasChClient_FindDbaaSClickhouseConnection_ConnectionNotFound() {
	yamlParams := configloader.YamlPropertySourceParams{ConfigFilePath: "testdata/application.yaml"}
	configloader.Init(configloader.BasePropertySources(yamlParams)...)
	AddHandler(Contains(getDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusNotFound)
	})
	ctx := context.Background()

	if _, err := suite.database.FindConnectionProperties(ctx); assert.Error(suite.T(), err) {
		assert.IsType(suite.T(), model.DbaaSCreateDbError{}, err)
		assert.Contains(suite.T(), err.Error(), "Incorrect response from DbaaS. Stop retrying")
		assert.Equal(suite.T(), 404, err.(model.DbaaSCreateDbError).HttpCode)
	}
}

func (suite *DatabaseTestSuite) TestServiceDbaasChClient_GetDbaaSClickhouseConnection() {
	AddHandler(Contains(createDatabaseV3), defaultDbaasResponseHandler)
	ctx := context.Background()

	actualResponse, err := suite.database.GetConnectionProperties(ctx)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), password, actualResponse.Password)
	assert.Equal(suite.T(), username, actualResponse.Username)
}

func (suite *DatabaseTestSuite) TestServiceDbaasChClient_GetDbaaSClickhouseConnection_ConnectionNotFound() {
	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusNotFound)
	})
	ctx := context.Background()

	if _, err := suite.database.GetConnectionProperties(ctx); assert.Error(suite.T(), err) {
		assert.IsType(suite.T(), model.DbaaSCreateDbError{}, err)
		assert.Contains(suite.T(), err.Error(), "Failed to get response from DbaaS")
		assert.Equal(suite.T(), 404, err.(model.DbaaSCreateDbError).HttpCode)
	}
}

func (suite *DatabaseTestSuite) TestServiceDbaasChClient_GetChClient_WithoutOptions() {
	actualChClient, err := suite.database.GetClickhouseClient()
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), actualChClient)
}

func (suite *DatabaseTestSuite) TestServiceDbaasChClient_GetChClient_WithSqlOptions() {
	opts := &clickhouse.Options{
		MaxOpenConns:    7,
		MaxIdleConns:    12,
		ConnMaxLifetime: 30 * time.Second,
	}
	actualChClient, err := suite.database.GetClickhouseClient(&chmodel.ClickhouseOptions{Options: opts})
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), actualChClient)
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(DatabaseTestSuite))
}

func defaultDbaasResponseHandler(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(http.StatusOK)
	connectionProperties := map[string]interface{}{
		"password": "qwerty127",
		"url":      "clickhouse://localhost:9000/test",
		"username": "service_test",
	}
	dbResponse := model.LogicalDb{
		Id:                   "123",
		ConnectionProperties: connectionProperties,
	}
	jsonResponse, _ := json.Marshal(dbResponse)
	writer.Write(jsonResponse)
}
