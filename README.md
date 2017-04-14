# Amazon DynamoDB Storage Backend for Titan

> Titan: Distributed Graph Database is a scalable graph database optimized for
> storing and querying graphs containing hundreds of billions of vertices and
> edges distributed across a multi-machine cluster. Titan is a transactional
> database that can support thousands of concurrent users executing complex
> graph traversals in real time. --
> [Titan Homepage](http://thinkaurelius.github.io/titan/)

> Amazon DynamoDB is a fast and flexible NoSQL database service for all
> applications that need consistent, single-digit millisecond latency at any
> scale. It is a fully managed database and supports both document and
> key-value data models. Its flexible data model and reliable performance make
> it a great fit for mobile, web, gaming, ad-tech, IoT, and many other
> applications.  -- [AWS DynamoDB Homepage](http://aws.amazon.com/dynamodb/)

Titan + DynamoDB = Distributed Graph Database - Cluster Host Management

[![Build Status](https://travis-ci.org/awslabs/dynamodb-titan-storage-backend.svg?branch=master)](https://travis-ci.org/awslabs/dynamodb-titan-storage-backend)

## Features
The following is a list of features of the Amazon DynamoDB Storage Backend for
Titan.
* AWS managed authentication and authorization.
* Configure table prefix to allow multiple graphs to be stored in a single
account in the same region.
* Full graph traversals with rate limited table scans.
* Flexible data model allows configuration between single-item and
multiple-item model based on graph size and utilization.
* Test graph locally with DynamoDB Local.
* Integrated with Titan metrics.
* Titan 1.0.0 and Tinkerpop 3.0.1-incubating compatibility.

## Getting Started
This example populates a Titan graph database backed by DynamoDB Local using
the
[Marvel Universe Social Graph](https://aws.amazon.com/datasets/5621954952932508).
The graph has a vertex per comic book character with an edge to each of the
comic books in which they appeared.

### Load a subset of the Marvel Universe Social Graph
1. Clone the repository in GitHub.

    ```
    git clone https://github.com/awslabs/dynamodb-titan-storage-backend.git
    ```
2. Run the `install` target to copy some dependencies to the target folder.

    ```
    mvn install
    ```
3. Start a new DynamoDB Local in a different shell.

    ```
    mvn test -Pstart-dynamodb-local
    ```
4. Clean up old Elasticsearch indexes.

    ```
    rm -rf /tmp/searchindex
    ```
5. Install Titan Server with the DynamoDB Storage Backend for Titan, which
includes Gremlin Server.

    ```
    src/test/resources/install-gremlin-server.sh
    ```
6. Change directories to the Gremlin Server home.

    ```
    cd server/dynamodb-titan100-storage-backend-1.0.3-hadoop1
    ```
7. Start Gremlin Server with the DynamoDB Local configuration.

    ```
    bin/gremlin-server.sh ${PWD}/conf/gremlin-server/gremlin-server-local.yaml
    ```
8. Start a Gremlin shell with `bin/gremlin.sh` and connect to the Gremlin Server
endpoint.

    ```
    :remote connect tinkerpop.server conf/remote.yaml
    ```
9. Load the first 100 lines of the Marvel graph using the Gremlin shell.

    ```
    :> com.amazon.titan.example.MarvelGraphFactory.load(graph, 100, false)
    ```
10. Print the characters and the comic-books they appeared in where the
characters had a weapon that was a shield or claws.

    ```
    :> g.V().has('weapon', within('shield','claws')).as('weapon', 'character', 'book').select('weapon', 'character','book').by('weapon').by('character').by(__.out('appeared').values('comic-book'))
    ```
11. Print the characters and the comic-books they appeared in where the
characters had a weapon that was not a shield or claws.

    ```
    :> g.V().has('weapon').has('weapon', without('shield','claws')).as('weapon', 'character', 'book').select('weapon', 'character','book').by('weapon').by('character').by(__.out('appeared').values('comic-book'))
    ```
12. Print a sorted list of the characters that appear in comic-book AVF 4.

    ```
    :> g.V().has('comic-book', 'AVF 4').in('appeared').values('character').order()
    ```
13. Print a sorted list of the characters that appear in comic-book AVF 4 that
have a weapon that is not a shield or claws.

    ```
    :> g.V().has('comic-book', 'AVF 4').in('appeared').has('weapon', without('shield','claws')).values('character').order()
    ```

### Load the Graph of the Gods
1. Repeat steps 1 through 8 of the Marvel graph section.
2. Load the Graph of the Gods.

    ```
    :> com.thinkaurelius.titan.example.GraphOfTheGodsFactory.load(graph)
    ```
3. Now you can follow the rest of the
[Titan Getting Started](http://s3.thinkaurelius.com/docs/titan/1.0.0/getting-started.html#_global_graph_indices)
documentation, starting from the Global Graph Indeces section. You need to
prepend each command with `:>` for remotely executing the commands on the
Gremlin Server endpoint. Also whenever you remotely execute traversals that
include local variables in steps, those local variables need to be defined in
the same line before the traversal. For example, to run the traversal
`g.V(hercules).out('father', 'mother').values('name')` remotely, you would need
to prepend it with the definition for Hercules:

    ```
    :> hercules = g.V(saturn).repeat(__.in('father')).times(2).next(); g.V(hercules).out('father', 'mother').values('name')
    ```
Note that the definition of Hercules depends on the definition of Saturn, so you
would need to define Saturn first:

    ```
    :> saturn = g.V().has('name', 'saturn').next(); hercules = g.V(saturn).repeat(__.in('father')).times(2).next(); g.V(hercules).out('father', 'mother').values('name')
    ```
The reason these need to be prepended is that local variable state is not
carried over for each remote script execution, except for the variables defined
in the scripts that run when Gremlin server is turned on. See the
`scriptEngines/gremlin-groovy/scripts` list element in the Gremlin Server YAML
file for more information.
4. Alternatively, repeat steps 1 through 8 of the Marvel graph section and
follow the examples in the
[Tinkerpop documentation](http://tinkerpop.incubator.apache.org/docs/3.0.1-incubating/#_mutating_the_graph),
prepending each command with `:>` for remote execution. Skip the
`TinkerGraph.open()` step as the remote execution environment already has a
`graph` variable set up.

### Run Gremlin on Gremlin Server in EC2 using a CloudFormation template
The DynamoDB Storage Backend for Titan includes a CloudFormation template that
creates a VPC, an EC2 instance in the VPC, installs Gremlin Server with the
DynamoDB Storage Backend for Titan installed, and starts the Gremlin Server
websockets endpoint. The Network ACL of the VPC includes just enough access to
allow:

 - you to connect to the instance using SSH and create tunnels (SSH inbound)
 - the EC2 instance to download yum updates from central repositories (HTTP
   outbound)
 - the EC2 instance to download your dynamodb.properties file and the Gremlin Server
   package from S3 (HTTPS outbound)
 - the EC2 instance to connect to DynamoDB (HTTPS outbound)
 - the ephemeral ports required to support the data flow above, in each
   direction

Requirements for running this CloudFormation template include two items.

 - You require an SSH key for EC2 instances must exist in the region you plan to
   create the Gremlin Server stack.
 - You need to have created an IAM role in the region that has S3 Read access
   and DynamoDB full access, the very minimum policies required to run this
   CloudFormation stack. S3 read access is required to provide the dynamodb.properties
   file to the stack in cloud-init. DynamoDB full access is required because the
   DynamoDB Storage Backend for Titan can create and delete tables, and read and
   write data in those tables.

Note, this cloud formation template downloads
[repackaged versions](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.TitanDB.GremlinServerEC2.html)
of the Titan zip files available on the
[Titan downloads page](https://github.com/thinkaurelius/titan/wiki/Downloads).
We repackaged these zip files in order to include the DynamoDB Storage Backend
for Titan and its dependencies.

1. Download the latest version of the CFN template from
[GitHub](https://github.com/awslabs/dynamodb-titan-storage-backend/blob/1.0.0/dynamodb-titan-storage-backend-cfn.json).
2. Navigate to the
[CloudFormation console](https://console.aws.amazon.com/cloudformation/home)
and click Create Stack.
3. On the Select Template page, name your Gremlin Server stack and select the
CloudFormation template that you just downloaded.
4. On the Specify Parameters page, you need to specify the following:
  * EC2 Instance Type
  * The network whitelist pattern for Gremlin Server Websockets port
  * The Gremlin Server port, default 8182.
  * The S3 URL to your dynamodb.properties configuration file
  * The name of your pre-existing EC2 SSH key
  * The network whitelist for the SSH protocol. You will need to allow incoming
  connections via SSH to enable the SSH tunnels that will secure Websockets connections
  to Gremlin Server.
  * The path to an IAM role that has the minimum amount of privileges to run this
  CloudFormation script and run Gremlin Server with the DynamoDB Storage Backend for
  Titan. This role will require S3 read to get the dynamodb.properties file, and DynamoDB full
  access to create tables and read and write items in those tables.
5. On the Options page, click Next.
6. On the Review page, select "I acknowledge that this template might cause AWS
CloudFormation to create IAM resources." Then, click Create.
7. Create an SSH tunnel from your localhost port 8182 to the Gremlin Server port (8182)
on the EC2 host after the stack deployment is complete. The SSH tunnel command
is one of the outputs of the CloudFormation script so you can just copy-paste it.
8. Repeat steps 5, 6, and 8 of the Marvel graph section above.

## Data Model
The Amazon DynamoDB Storage Backend for Titan has a flexible data model that
allows clients to select the data model for each Titan backend table. Clients
can configure tables to use either a single-item model or a multiple-item model.

### Single-Item Model
The single-item model uses a single DynamoDB item to store all values for a
single key.  In terms of Titan backend implementations, the key becomes the
DynamoDB hash key, and each column becomes an attribute name and the column
value is stored in the respective attribute value.

This is definitely the most efficient implementation, but beware of the 400kb
limit DynamoDB imposes on items. It is best to only use this on tables you are
sure will not surpass the item size limit. Graphs with low vertex degree and
low number of items per index can take advantage of this implementation.

### Multiple-Item Model
The multiple-item model uses multiple DynamoDB items to store all values for a
single key.  In terms of Titan backend implementations, the key becomes the
DynamoDB hash key, and each column becomes the range key in its own item.
The column values are stored in its own attribute.

The multiple item model is less efficient than the single-item during initial
graph loads, but it gets around the 400kb limitation. The multiple-item model
uses range Query calls instead of GetItem calls to get the necessary column
values.

## DynamoDB Specific Configuration
Each configuration option has a certain mutability level that governs whether
and how it can be modified after the database is opened for the first time. The
following listing describes the mutability levels.

1. **FIXED** - Once the database has been opened, these configuration options
cannot be changed for the entire life of the database
2. **GLOBAL_OFFLINE** - These options can only be changed for the entire
database cluster at once when all instances are shut down
3. **GLOBAL** - These options can only be changed globally across the entire
database cluster
4. **MASKABLE** - These options are global but can be overwritten by a local
configuration file
5. **LOCAL** - These options can only be provided through a local configuration
file

Leading namespace names are shortened and sometimes spaces were inserted in long
strings to make sure the tables below are formatted correctly.

### General DynamoDB Configuration Parameters
All of the following parameters are in the `storage` (`s`) namespace, and most
are in the `storage.dynamodb` (`s.d`) namespace subset.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.backend` | The primary persistence provider used by Titan. To use DynamoDB you must set this to `com.amazon.titan.diskstorage. dynamodb.DynamoDBStoreManager` | String |  | MASKABLE |
| `s.d.prefix` | A prefix to put before the Titan table name. This allows clients to have multiple graphs in the same AWS DynamoDB account in the same region. | String | titan | FIXED |
| `s.d.metrics-prefix` | Prefix on the codahale metric names emitted by DynamoDBDelegate. | String | dynamodb | MASKABLE |
| `s.d.force-consistent-read` | This feature sets the force consistent read property on DynamoDB calls. | Boolean | true | MASKABLE |
| `s.d.enable-parallel-scan` | This feature changes the scan behavior from a sequential scan (with consistent key order) to a segmented, parallel scan. Enabling this feature will make full graph scans faster, but it may cause this backend to be incompatible with Titan's OLAP library. | Boolean | false | MASKABLE |
| `s.d.max-self-throttled-retries` | The number of retries that the backend should attempt and self-throttle. | Integer | 60 | MASKABLE |
| `s.d.initial-retry-millis` | The amount of time to initially wait (in milliseconds) when retrying self-throttled DynamoDB API calls. | Integer | 25 | MASKABLE |
| `s.d.control-plane-rate` | The rate in permits per second at which to issue DynamoDB control plane requests (CreateTable, UpdateTable, DeleteTable, ListTables, DescribeTable). | Double | 10 | MASKABLE |

### DynamoDB KeyColumnValue Store Configuration Parameters
Some configurations require specifications for each of the Titan backend
Key-Column-Value stores. Here is a list of the default Titan backend
Key-Column-Value stores:
* edgestore
* graphindex
* titan_ids
* system_properties
* systemlog
* txlog

In addition, any store you define in the umbrella `storage.dynamodb.stores.*`
namespace that starts with `ulog_` will be used for user-defined transaction
logs.

You can configure the initial read and write capacity, rate limits, scan limits and
data model for each KCV graph store. You can always scale up and down the read and
write capacity of your tables in the DynamoDB console. If you have a write once,
read many workload, or you are running a bulk data load, it is useful to adjust the
capacity of edgestore and graphindex tables as necessary in the DynamoDB console,
and decreasing the allocated capacity and rate limiters afterwards.

For details about these Key-Column-Value stores, please see
[Store Mapping](https://github.com/elffersj/delftswa-aurelius-titan/blob/master/SA-doc/Mapping.md)
and
[Titan Data Model](http://s3.thinkaurelius.com/docs/titan/current/data-model.html).
All of these configuration parameters are in the `storage.dynamodb.stores`
(`s.d.s`) umbrella namespace subset. In the tables below these configurations
have the text `t` where the Titan store name should go.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.s.t.data-model` | SINGLE means that all the values for a given key are put into a single DynamoDB item.  A SINGLE is efficient because all the updates for a single key can be done atomically. However, the tradeoff is that DynamoDB has a 400k limit per item so it cannot hold much data. MULTI means that each 'column' is used as a range key in DynamoDB so a key can span multiple items. A MULTI implementation is slightly less efficient than SINGLE because it must use DynamoDB Query rather than a direct lookup. It is HIGHLY recommended to use MULTI for edgestore and graphindex unless your graph has very low max degree.| String | MULTI | FIXED |
| `s.d.s.t.capacity-read` | Define the initial read capacity for a given DynamoDB table. Make sure to replace the `s` with your actual table name. | Integer | 4 | GLOBAL |
| `s.d.s.t.capacity-write` | Define the initial write capacity for a given DynamoDB table. Make sure to replace the `s` with your actual table name. | Integer | 4 | GLOBAL |
| `s.d.s.t.read-rate` | The max number of reads per second. | Double | 4 | MASKABLE |
| `s.d.s.t.write-rate` | Used to throttle write rate of given table. The max number of writes per second. | Double | 4 | MASKABLE |
| `s.d.s.t.scan-limit` | The maximum number of items to evaluate (not necessarily the number of matching items). If DynamoDB processes the number of items up to the limit while processing the results, it stops the operation and returns the matching values up to that point, and a key in LastEvaluatedKey to apply in a subsequent operation, so that you can pick up where you left off. Also, if the processed data set size exceeds 1 MB before DynamoDB reaches this limit, it stops the operation and returns the matching values up to the limit, and a key in LastEvaluatedKey to apply in a subsequent operation to continue the operation. | Integer | 10000 | MASKABLE |

### DynamoDB Client Configuration Parameters
All of these configuration parameters are in the `storage.dynamodb.client`
(`s.d.c`) namespace subset, and are related to the DynamoDB SDK client
configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.connection-timeout` | The amount of time to wait (in milliseconds) when initially establishing a connection before giving up and timing out. | Integer | 60000 | MASKABLE |
| `s.d.c.connection-ttl` | The expiration time (in milliseconds) for a connection in the connection pool. | Integer | 60000 | MASKABLE |
| `s.d.c.connection-max` |  The maximum number of allowed open HTTP connections.| Integer | 10 | MASKABLE |
| `s.d.c.retry-error-max` |  The maximum number of retry attempts for failed retryable requests (ex: 5xx error responses from services).| Integer | 0 | MASKABLE |
| `s.d.c.use-gzip` |   Sets whether gzip compression should be used. | Boolean | false | MASKABLE |
| `s.d.c.use-reaper` |  Sets whether the IdleConnectionReaper is to be started as a daemon thread. | Boolean | true | MASKABLE |
| `s.d.c.user-agent` | The HTTP user agent header to send with all requests.| String | | MASKABLE |
| `s.d.c.endpoint` | Sets the endpoint for this client. | String | | MASKABLE |

#### DynamoDB Client Proxy Configuration Parameters
All of these configuration parameters are in the
`storage.dynamodb.client.proxy` (`s.d.c.p`) namespace subset, and are related to
the DynamoDB SDK client proxy configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.p.domain` | The optional Windows domain name for configuration an NTLM proxy.| String | | MASKABLE |
| `s.d.c.p.workstation` | The optional Windows workstation name for configuring NTLM proxy support.| String | | MASKABLE |
| `s.d.c.p.host` | The optional proxy host the client will connect through.| String | | MASKABLE |
| `s.d.c.p.port` | The optional proxy port the client will connect through.| String | | MASKABLE |
| `s.d.c.p.username` | The optional proxy user name to use if connecting through a proxy.| String | | MASKABLE |
| `s.d.c.p.password` |  The optional proxy password to use when connecting through a proxy.| String | | MASKABLE |

#### DynamoDB Client Socket Configuration Parameters
All of these configuration parameters are in the `storage.dynamodb.client.socket`
(`s.d.c.s`) namespace subset, and are related to the DynamoDB SDK client socket
configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.s.buffer-send-hint` | The optional size hints (in bytes) for the low level TCP send and receive buffers.| Integer | 1048576 | MASKABLE |
| `s.d.c.s.buffer-recv-hint` | The optional size hints (in bytes) for the low level TCP send and receive buffers.| Integer | 1048576 | MASKABLE |
| `s.d.c.s.timeout` | The amount of time to wait (in milliseconds) for data to be transfered over an established, open connection before the connection times out and is closed.| Long | 50000 | MASKABLE |
| `s.d.c.s.tcp-keep-alive` | Sets whether or not to enable TCP KeepAlive support at the socket level. Not used at the moment. | Boolean |  | MASKABLE |

#### DynamoDB Client Executor Configuration Parameters
All of these configuration parameters are in the `storage.dynamodb.client.executor`
(`s.d.c.e`) namespace subset, and are related to the DynamoDB SDK client
executor / thread-pool configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.e.core-pool-size` |  The core number of threads for the DynamoDB async client. | Integer | `Runtime.getRuntime(). availableProcessors() * 2` | MASKABLE |
| `s.d.c.e.max-pool-size` | The maximum allowed number of threads for the DynamoDB async client. | Integer | `Runtime.getRuntime(). availableProcessors() * 4` | MASKABLE |
| `s.d.c.e.keep-alive` | The time limit for which threads may remain idle before being terminated for the DynamoDB async client.  | Integer | | MASKABLE |
| `s.d.c.e.max-queue-length` | The maximum size of the executor queue before requests start getting run in the caller.  | Integer | 1024 | MASKABLE |
| `s.d.c.e.max-concurrent-operations` | The expected number of threads expected to be using a single TitanGraph instance. Used to allocate threads to batch operations. | Integer | 1 | MASKABLE |

#### DynamoDB Client Credential Configuration Parameters
All of these configuration parameters are in the `storage.dynamodb.client.credentials`
(`s.d.c.c`) namespace subset, and are related to the DynamoDB SDK client
credential configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.c.class-name` | Specify the fully qualified class that implements AWSCredentialsProvider or AWSCredentials. | String | `com.amazonaws.auth. BasicAWSCredentials` | MASKABLE |
| `s.d.c.c.constructor-args` | Comma separated list of strings to pass to the credentials constructor. | String | `accessKey,secretKey` | MASKABLE |

## Run all tests against DynamoDB Local on an EC2 Amazon Linux AMI
1. Install dependencies. For Amazon Linux:

    ```
    sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
    sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
    sudo yum update -y && sudo yum upgrade -y
    sudo yum install -y apache-maven sqlite-devel git java-1.8.0-openjdk-devel
    sudo alternatives --set java /usr/lib/jvm/jre-1.8.0-openjdk.x86_64/bin/java
    sudo alternatives --set javac /usr/lib/jvm/java-1.8.0-openjdk.x86_64/bin/javac
    ```
2. Open a screen so that you can log out of the EC2 instance while running tests with `screen`.
3. Run the single-item data model tests.

    ```
    mvn verify -P integration-tests -Dexclude.category=com.amazon.titan.testcategory.MultipleItemTests -Dinclude.category="**/*.java" > o 2>&1
    ```
4. Run the multiple-item data model tests.

    ```
    mvn verify -P integration-tests -Dexclude.category=com.amazon.titan.testcategory.SingleItemTests -Dinclude.category="**/*.java" > o 2>&1
    ```
5. Exit the screen with `CTRL-A D` and logout of the EC2 instance.
6. Monitor the CPU usage of your EC2 instance in the EC2 console. The single-item tests
may take at least 1 hour and the multiple-item tests may take at least 2 hours to run.
When CPU usage goes to zero, that means the tests are done.
7. Log back into the EC2 instance and resume the screen with `screen -r` to
review the test results.

    ```
    cd target/surefire-reports && grep testcase *.xml | grep -v "\/"
    ```
8. Terminate the instance when done.
