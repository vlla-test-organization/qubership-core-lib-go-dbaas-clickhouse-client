package clickhousedbaas

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/docker/go-connections/nat"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/cache"
	dbaasbasemodel "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
	. "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/testutils"
	"github.com/netcracker/qubership-core-lib-go-dbaas-clickhouse-client/model"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	clickhousePort          = "9000"
	testContainerDbPassword = "ClickHouse"
	testContainerDbUser     = "ClickhouseUser"
	testContainerDb         = "ClickhouseUserDb"
	wrongPassword           = "qwerty123"
)

// entity for database tests
type Book struct {
	Code string
}

func (suite *DatabaseTestSuite) TestChClient_GetConnection_ConnectionError() {
	ctx := context.Background()

	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := clickhouseDbaasResponseHandler("localhost:65000", testContainerDbPassword)
		writer.Write(jsonString)
	})

	params := model.DbParams{Classifier: ServiceClassifier, BaseDbParams: rest.BaseDbParams{Role: "admin"}}
	chClient := chClientImpl{
		params:          params,
		clickhouseCache: &cache.DbaaSCache{LogicalDbCache: make(map[cache.Key]interface{})},
		dbaasClient:     dbaasbase.NewDbaasClient(),
	}
	conn, err := chClient.GetConnection(ctx)
	assert.Nil(suite.T(), conn)
	assert.NotNil(suite.T(), err)
}

func (suite *DatabaseTestSuite) TestChClient_GetConnection_NewClient() {
	ctx := context.Background()
	port, _ := nat.NewPort("tcp", clickhousePort)
	container := prepareTestContainer(suite.T(), ctx, port)
	defer func() {
		err := container.Terminate(ctx)
		if err != nil {
			suite.T().Fatal(err)
		}
	}()

	addr, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		suite.T().Error(err)
	}

	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := clickhouseDbaasResponseHandler(addr, testContainerDbPassword)
		writer.Write(jsonString)
	})

	params := model.DbParams{Classifier: ServiceClassifier, BaseDbParams: rest.BaseDbParams{Role: "admin"}}
	chClient := chClientImpl{
		params:          params,
		clickhouseCache: &cache.DbaaSCache{LogicalDbCache: make(map[cache.Key]interface{})},
		dbaasClient:     dbaasbase.NewDbaasClient(),
	}
	dbBun, err := chClient.GetConnection(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), dbBun)
	// check that connection allows storing and getting info from db
	suite.checkConnectionIsWorking(dbBun, ctx)
}

func (suite *DatabaseTestSuite) TestChClient_GetConnection_ClientFromCache() {
	ctx := context.Background()
	port, _ := nat.NewPort("tcp", clickhousePort)
	container := prepareTestContainer(suite.T(), ctx, port)
	defer func() {
		err := container.Terminate(ctx)
		if err != nil {
			suite.T().Fatal(err)
		}
	}()

	addr, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		suite.T().Error(err)
	}

	counter := 0
	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := clickhouseDbaasResponseHandler(addr, testContainerDbPassword)
		writer.Write(jsonString)
		counter++
	})

	params := model.DbParams{Classifier: ServiceClassifier, BaseDbParams: rest.BaseDbParams{Role: "admin"}}
	chClient := chClientImpl{
		params:          params,
		clickhouseCache: &cache.DbaaSCache{LogicalDbCache: make(map[cache.Key]interface{})},
		dbaasClient:     dbaasbase.NewDbaasClient(),
	}
	firstConn, err := chClient.GetConnection(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), firstConn)
	assert.Equal(suite.T(), 1, counter)

	// check that connection allows storing and getting info from db
	suite.checkConnectionIsWorking(firstConn, ctx)

	secondConn, err := chClient.GetConnection(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), secondConn)
	assert.Equal(suite.T(), 1, counter)

	// check that connection allows storing and getting info from db
	suite.checkConnectionIsWorking(secondConn, ctx)
}

func (suite *DatabaseTestSuite) TestChClient_GetConnection_UpdatePassword() {
	ctx := context.Background()
	port, _ := nat.NewPort("tcp", clickhousePort)
	container := prepareTestContainer(suite.T(), ctx, port)
	defer func() {
		err := container.Terminate(ctx)
		if err != nil {
			suite.T().Fatal(err)
		}
	}()
	addr, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		suite.T().Error(err)
	}

	// create database with wrong password
	AddHandler(matches(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := clickhouseDbaasResponseHandler(addr, wrongPassword)
		writer.Write(jsonString)
	})
	// update right password
	AddHandler(matches(getDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := clickhouseDbaasResponseHandler(addr, testContainerDbPassword)
		writer.Write(jsonString)
	})
	params := model.DbParams{Classifier: ServiceClassifier, BaseDbParams: rest.BaseDbParams{Role: "admin"}}
	chClient := chClientImpl{
		params:          params,
		clickhouseCache: &cache.DbaaSCache{LogicalDbCache: make(map[cache.Key]interface{})},
		dbaasClient:     dbaasbase.NewDbaasClient(),
	}

	conn, err := chClient.GetConnection(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), conn)

	// check that connection allows storing and getting info from db
	suite.checkConnectionIsWorking(conn, ctx)
}

func (suite *DatabaseTestSuite) checkConnectionIsWorking(conn driver.Conn, ctx context.Context) {
	errCreate := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS books (
			  Code String
		) Engine = Memory
		`)
	assert.Nil(suite.T(), errCreate)
	errInsert := conn.Exec(ctx, "INSERT INTO books VALUES ('111')")
	assert.Nil(suite.T(), errInsert)
	bookForSelect := make([]Book, 0)
	errSelect := conn.Select(ctx, &bookForSelect, "SELECT Code FROM books")
	assert.Nil(suite.T(), errSelect)
	assert.Equal(suite.T(), 1, len(bookForSelect))
	assert.Equal(suite.T(), "111", bookForSelect[0].Code)
	errDrop := conn.Exec(ctx, "DROP TABLE IF EXISTS books")
	assert.Nil(suite.T(), errDrop)
}

func (suite *DatabaseTestSuite) TestReconnectOnTcpTearDown() {
	ctx := context.Background()
	port, _ := nat.NewPort("tcp", clickhousePort)
	container := prepareTestContainer(suite.T(), ctx, port)
	defer func() {
		err := container.Terminate(ctx)
		if err != nil {
			suite.T().Fatal(err)
		}
	}()
	addr, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		suite.T().Error(err)
	}
	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := clickhouseDbaasResponseHandler(addr, testContainerDbPassword)
		writer.Write(jsonString)
	})
	params := model.DbParams{Classifier: ServiceClassifier, BaseDbParams: rest.BaseDbParams{Role: "admin"}}
	chClient := chClientImpl{
		params:          params,
		clickhouseCache: &cache.DbaaSCache{LogicalDbCache: make(map[cache.Key]interface{})},
		dbaasClient:     dbaasbase.NewDbaasClient(),
	}
	conn, err := chClient.GetConnection(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), conn)
	//  drop tcp connections of the cached dbaas ch connection
	stopDuration := 5 * time.Second
	assert.Nil(suite.T(), container.Stop(ctx, &stopDuration))
	assert.Nil(suite.T(), container.Start(ctx))

	addr, err = container.PortEndpoint(ctx, port, "")
	if err != nil {
		suite.T().Error(err)
	}
	conn, err = chClient.GetConnection(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), conn)
}

func matches(submatch string) func(string) bool {
	return func(path string) bool {
		return strings.EqualFold(path, submatch)
	}
}

func clickhouseDbaasResponseHandler(address, password string) []byte {
	url := fmt.Sprintf("clickhouse://%s/%s", address, testContainerDb)
	connectionProperties := map[string]interface{}{
		"password": password,
		"url":      url,
		"username": testContainerDbUser,
	}
	dbResponse := dbaasbasemodel.LogicalDb{
		Id:                   "123",
		ConnectionProperties: connectionProperties,
	}
	jsonResponse, _ := json.Marshal(dbResponse)
	return jsonResponse
}

func prepareTestContainer(t *testing.T, ctx context.Context, port nat.Port) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:24.1.8.22",
		ExposedPorts: []string{port.Port(), "8123/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForHTTP("/ping").WithPort("8123/tcp").WithStatusCodeMatcher(
				func(status int) bool {
					return status == http.StatusOK
				},
			),
		),
		Env: map[string]string{
			"CLICKHOUSE_DB":       testContainerDb,
			"CLICKHOUSE_USER":     testContainerDbUser,
			"CLICKHOUSE_PASSWORD": testContainerDbPassword,
		},
	}
	clickhouseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}

	dbHost, _ := clickhouseContainer.Host(ctx)
	dbPort, _ := clickhouseContainer.MappedPort(ctx, port)
	connString := fmt.Sprintf("tcp://%s:%s@%s:%s", testContainerDbUser, testContainerDbPassword, dbHost, dbPort.Port())
	conn, err := sql.Open("clickhouse", connString)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Exec("CREATE DATABASE IF NOT EXISTS " + testContainerDb)
	if err != nil {
		t.Fatal(err)
	}

	return clickhouseContainer
}
