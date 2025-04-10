[![Coverage](https://sonarcloud.io/api/project_badges/measure?metric=coverage&project=Netcracker_qubership-core-lib-go-dbaas-clickhouse-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-dbaas-clickhouse-client)
[![duplicated_lines_density](https://sonarcloud.io/api/project_badges/measure?metric=duplicated_lines_density&project=Netcracker_qubership-core-lib-go-dbaas-clickhouse-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-dbaas-clickhouse-client)
[![vulnerabilities](https://sonarcloud.io/api/project_badges/measure?metric=vulnerabilities&project=Netcracker_qubership-core-lib-go-dbaas-clickhouse-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-dbaas-clickhouse-client)
[![bugs](https://sonarcloud.io/api/project_badges/measure?metric=bugs&project=Netcracker_qubership-core-lib-go-dbaas-clickhouse-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-dbaas-clickhouse-client)
[![code_smells](https://sonarcloud.io/api/project_badges/measure?metric=code_smells&project=Netcracker_qubership-core-lib-go-dbaas-clickhouse-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-dbaas-clickhouse-client)

# Clickhouse dbaas go client

[adviser=Sofiia Shalgueva, owner=Artemii Vorobev]

This module provides convenient way of interaction with **clickhouse** databases provided by dbaas-aggregator.
`Clickhouse dbaas go client` supports _multi-tenancy_ and can work with both _service_ and _tenant_ databases.

This module is based on [clickhouse-go](https://github.com/ClickHouse/clickhouse-go) library.

- [Install](#install)
- [Usage](#usage)
    * [Get connection for existing database or create new one](#get-connection-for-existing-database-or-create-new-one)
    * [Find connection for existing database](#find-connection-for-existing-database)
    * [ClickhouseDbClient](#clickhousedbclient)
- [Classifier](#classifier)
- [Clickhouse multiusers](#clickhouse-multiusers)
- [SSL/TLS support](#ssltls-support)
- [Quick example](#quick-example)

## Install
To get `clickhouse dbaas client` use
```go
 go get github.com/netcracker/qubership-core-lib-go-dbaas-clickhouse-client@<latest released version>
```

List of all released versions may be found [here](https://github.com/netcracker/qubership-core-lib-go-dbaas-clickhouse-client/-/tags)

## Usage

At first, it's necessary to register security implemention - dummy or your own, the followning example shows registration of required services:
```go
import (
	"github.com/netcracker/qubership-core-lib-go/v3/serviceloader"
	"github.com/netcracker/qubership-core-lib-go/v3/security"
)

func init() {
	serviceloader.Register(1, &security.DummyToken{})
	serviceloader.Register(1, &security.TenantContextObject{})
}
```

Then the user should create `DbaaSClickhouseClient`. This is a base client, which allows working with tenant and service databases.
To create instance of `DbaaSClickhouseClient` use `NewClient(pool *dbaasbase.DbaaSPool) *DbaaSClickhouseClient`.

Note that client has parameter _pool_. `dbaasbase.DbaaSPool` is a tool which stores all cached connections and
create new ones. To find more info visit [dbaasbase](https://github.com/netcracker/qubership-core-lib-go-dbaas-base-client/blob/main/README.md)

Example of client creation:
```go
pool := dbaasbase.NewDbaasPool()
client := clickhousedbaas.NewClient(pool)
```

By default, Clickhouse dbaas go client supports dbaas-aggregator as databases source. But there is a possibility for user to provide another
sources (for example, zookeeper). To do so use [LogcalDbProvider](https://github.com/netcracker/qubership-core-lib-go-dbaas-base-client/blob/main/README.md#logicaldbproviders)
from dbaasbase.

Next step is to create `Database` object. `Database` is not a clickhouse.Conn instance. It just an interface which allows
creating ClickhouseDbClient or getting connection properties from dbaas. At this step user may choose which type of database he will
work with: `service` or `tenant`.

* To work with service databases use `ServiceDatabase(params ...model.DbParams) Database`
* To work with tenant databases use `TenantDatabase(params ...model.DbParams) Database`

Each func has `DbParams` as parameter.

DbParams store information for database creation. Note that this parameter is optional, but if user doesn't pass Classifier,
default one will be used. More about classifiers [here](#classifier)

| Name         | Description                                                                                        | type                                                                                                                       |
|--------------|----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| Classifier   | function which builds classifier from context. Classifier should be unique for each clickhouse db. | func(ctx context.Context) map[string]interface{}                                                                           |
| BaseDbParams | Specific parameters for database creation.                                                         | [BaseDbParams](https://github.com/netcracker/qubership-core-lib-go-dbaas-base-client/blob/main#basedbparams)   | 

Example how to create an instance of Database.
```go
 dbPool := dbaasbase.NewDbaasPool()
 client := clickhousedbaas.NewClient(dbPool)
 serviceDB := client.ServiceDatabase() // service Database creation 
 tenantDB := client.TenantDatabase() // tenant client creation 
```

`Database` allows: create new database and get connection to it, get connection to existing database and create ClickhouseDbClient. `serviceDB` and `tenantDB` should be singleton and
it's enough to create them only once.

### Get connection for existing database or create new one

Func `GetConnectionProperties(ctx context.Context) (*model.ClickhouseConnProperties, error)`
at first will check if the desired database with _clickhouse_ type and classifier exists. If it exists, function will just return
connection properties in the form of [ClickhouseConnProperties](model/ch_conn_properties.go).
If database with _clickhouse_ type and classifier doesn't exist, such database will be created and function will return
connection properties for a new created database.

_Parameters:_
* ctx - context, enriched with some headers (See docs about context-propagation [here](https://github.com/netcracker/qubership-core-lib-go/blob/main/context-propagation/README.md)).
  Context object can have request scope values from which can be used to build classifier, for example tenantId.

```go
    ctx := ctxmanager.InitContext(context.Background(), propagateHeaders()) // preferred way
    // ctx := context.Background() // also possible for service client, but not recommended
    dbClickhouseConnection, err := database.GetConnectionProperties(ctx)
```

### Find connection for existing database

Func `FindConnectionProperties(ctx context.Context) (*model.ClickhouseConnProperties, error)`
returns connection properties in the form of [ClickhouseConnProperties](model/ch_conn_properties.go). Unlike `GetConnectionProperties`
this function won't create database if it doesn't exist and just return nil value.

_Parameters:_
* ctx - context, enriched with some headers. (See docs about context-propagation [here](https://github.com/netcracker/qubership-core-lib-go/blob/main/context-propagation/README.md)). 
  Context object can have request scope values from which can be used to build classifier, for example tenantId.

```go
    ctx := ctxmanager.InitContext(context.Background(), propagateHeaders()) // preferred way
    // ctx := context.Background() // also possible for service client, but not recommended
    dbClickhouseConnection, err := database.FindConnectionProperties(ctx)
```

### ClickhouseDbClient

ClickhouseDbClient is a special object, which allows getting `driver.Conn` to establish connection and to operate with a database. `
ClickhouseDbClient` is a singleton and should be created only once.

ClickhouseDbClient has API:
* `GetConnection(ctx context.Context) (driver.Conn, error)` which will return `driver.Conn`

We strongly recommend not to store any of these objects as singleton and get new connection for every block of code.
This is because the password in the database may change and then the connection will return an error. Every time the function
`ClickhouseDbClient.GetConnection()`is called, the password lifetime and correctness is checked. If necessary, the password is updated.

Note that: classifier will be created with context and function from DbParams.

To create ClickhouseDbClient use `GetClickhouseClient(options ...*model.ClickhouseOptions) (ClickhouseDbClient, error)`

Parameters:
* options *model.ClickhouseOptions _optional_ - user may pass some desired configuration for `driver.Conn` or don't pass anything at all.
  model.ClickhouseOptions contains such fields as:
    - clickhouse.Options - Options for `driver.Conn` object creation. User **should not** pass credentials with this options - credentials will be received from dbaas-aggregator.
      With these options user may set such parameters as _DialTimeout_, _MaxOpenConns_, _ConnMaxLifetime_, etc.


```go
    ctx := ctxmanager.InitContext(context.Background(), propagateHeaders()) // preferred way
    // ctx := context.Background() // also possible for service client, but not recommended
    opts := &clickhouse.Options{
      MaxOpenConns:    7,
      MaxIdleConns:    12,
      ConnMaxLifetime: 30 * time.Second,
    } 
    clickhouseDbClient, err := database.GetClickhouseClient(&chmodel.ClickhouseOptions{Options: opts}}) // with options
    connection, err := ClickhouseDbClient.GetConnection(ctx)
```

## Classifier

Classifier and dbType should be unique combination for each database.  Fields "tenantId" or "scope" must be into users' custom classifiers.

User can use default service or tenant classifier. It will be used if user doesn't specify Classifier in DbParams. 
Fields in classifiers below are mandatory to use.

Default service classifier looks like:
```json
{
    "scope": "service",
    "microserviceName": "<ms-name>",
    "namespace" : "<ms-namespace>"
}
```

Default tenant classifier looks like:
```json
{
  "scope" : "tenant",
  "tenantId": "<tenant-external-id>",
  "microserviceName": "<ms-name>",
  "namespace" : "<ms-namespace>"
}
```
Note, that if user doesn't set `MICROSERVICE_NAME` (or `microservice.name`) property, there will be panic during default classifier creation.
Also, if there are no tenantId in tenantContext, **panic will be thrown**.

Users may create their own custom classifiers. Please note, that all fields from default classifiers should 
still be present in custom ones. The easiest way is to take the basic classifier and add required custom fields.

Example of custom classifier creation:
```go
classifier := func(ctx context.Context) map[string]interface{} {
  classifier := TenantClassifier(ctx) // or ServiceClassifier(ctx)
  classifier["logicalDbId"] = "internal-Id"
  return classifier
}
params := model.DbParams{
    Classifier:   classifier, //database classifier func
}
```

## Clickhouse multiusers
For specifying connection properties user role you should add this role in BaseDbParams structure:

```go
  params := model.DbParams{
          BaseDbParams: rest.BaseDbParams{Role: "admin"}, //for example "admin", "rw", "ro"
      }
  dbPool := dbaasbase.NewDbaaSPool()
  clickhouseClient := clickhousedbaas.NewClient(dbPool)
  serviceDb := clickhouseClient.ServiceDatabase(params) //or for tenant database - TenantDatabase(params)
  chClient, err := serviceDb.GetClickhouseClient()
  conn, err := chClient.GetConnection(ctx)
```
Requests to DbaaS will contain the role you specify in this structure.

## SSL/TLS support

This library supports work with secured connections to clickhouse. Connection will be secured if TLS mode is enabled in
clickhouse-adapter.

For correct work with secured connections, the library requires having a truststore with certificate.
It may be public cloud certificate, cert-manager's certificate or any type of certificates related to database.
We do not recommend use self-signed certificates. Instead, use default NC-CA.

To start using TLS feature user has to enable it on the physical database (adapter's) side and add certificate to service truststore.

### Physical database switching

To enable TLS support in physical database redeploy clickhouse with mandatory parameters
```yaml
tls.enabled=true;
```

In case of using cert-manager as certificates source add extra parameters
```yaml
tls.generateCerts.enabled=true
tls.generateCerts.clusterIssuerName=<cluster issuer name>
```

ClusterIssuername identifies which Certificate Authority cert-manager will use to issue a certificate.
It can be obtained from the person in charge of the cert-manager on the environment.

## Quick example

Here we create clickhouse tenant client, then get clickhouseDbClient and execute a query for table creation.

application.yaml
```yaml
application.yaml
  
  microservice.name=tenant-manager
```

```go
package main

import (
	"context"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/ctxmanager"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
	clickhousedbaas "github.com/netcracker/qubership-core-lib-go-dbaas-clickhouse-client/v2"
	"github.com/ClickHouse/clickhouse-go/v2"
)

var logger logging.Logger

func init() {
	configloader.Init(configloader.BasePropertySources())
	logger = logging.GetLogger("main")
	ctxmanager.Register([]ctxmanager.ContextProvider{tenantcontext.TenantProvider{}})
}

type Book struct {
  Code int
}

func main() { 
	// some context initialization 
	ctx := ctxmanager.InitContext(context.Background(), map[string]interface{}{tenant.TenantContextName: "id"})

    dbPool := dbaasbase.NewDbaaSPool()
    chDbClient := clickhousedbaas.NewClient(dbPool)
    db := chDbClient.TenantDatabase()
    client, _ := db.GetClickhouseClient()  // singleton for tenant db. This object must be used to get connection in the entire application.
	
    db, err := client.GetConnection(ctx) // now we can get driver.Conn and work with queries
    if err != nil {
        logger.Panicf("Got error during connection creation %+v", err)
    }
    errCreate := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS example (
			  Code String
		) Engine = Memory
		`)
	if errCreate != nil {
      logger.Panicf("Got error during table creation %+v", errCreate)
    }
    errInsert := conn.Exec(ctx, "INSERT INTO example VALUES ('111')")
    if err != nil {
      logger.Panicf("Got error during connection creation %+v", errInsert)
    }
}
```
