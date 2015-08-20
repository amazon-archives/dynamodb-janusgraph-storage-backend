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
* Titan 0.4.4 compatibility.

## Getting Started
This example populates a Titan graph database backed by DynamoDB Local using
the
[Marvel Universe Social Graph](https://aws.amazon.com/datasets/5621954952932508).
The graph has a vertex per comic book character with an edge to each of the
comic books in which they appeared.

### Load a subset of the Marvel Universe Social Graph
1. Clone the repository in GitHub.

    ```
    git clone https://github.com/awslabs/dynamodb-titan-storage-backend.git && cd dynamodb-titan-storage-backend && git checkout 0.4.4
    ```
2. Run the `install` target to copy some dependencies to the target folder.

    ```
    mvn install
    ```
3. Start DynamoDB Local in a different shell.

    ```
    mvn test -Pstart-dynamodb-local
    ```
4. Run the Gremlin shell.

    ```
    mvn test -Pstart-gremlin
    ```
5. Open a graph using the Titan DynamoDB Storage Backend in the Gremlin shell.

    ```
    conf = new BaseConfiguration()
    conf.setProperty("storage.backend", "com.amazon.titan.diskstorage.dynamodb.DynamoDBStoreManager")
    conf.setProperty("storage.dynamodb.client.endpoint", "http://localhost:4567")
    conf.setProperty("storage.index.search.backend", "elasticsearch")
    conf.setProperty("storage.index.search.directory", "/tmp/searchindex")
    conf.setProperty("storage.index.search.client-only", "false")
    conf.setProperty("storage.index.search.local-mode", "true")
    g = TitanFactory.open(conf)
    ```
6. Load the first 100 lines of the Marvel graph.

    ```
    com.amazon.titan.example.MarvelGraphFactory.load(g, 100, false)
    ```
7. Print the characters and the comic-books they appeared in where the characters had a weapon that was a shield or claws.

    ```
    g.V.has('weapon', T.in, ['shield','claws']).as('c').outE('appeared').inV.as('b').transform{e,m -> m.c.character + ' had ' + m.c.weapon + ' in ' + m.b['comic-book']}.order
    ```
8. Print the characters and the comic-books they appeared in where the characters had a weapon that was not a shield or claws.

    ```
    g.V.has('weapon', T.notin, ['shield','claws']).as('c').outE('appeared').inV.as('b').transform{e,m -> m.c.character + ' had ' + m.c.weapon + ' in ' + m.b['comic-book']}.order
    ```
9. Print a sorted list of the characters that appear in comic-book AVF 4.

    ```
    g.V.has('comic-book', 'AVF 4').inE('appeared').outV.character.order
    ```
10. Print a sorted list of the characters that appear in comic-book AVF 4 that have a weapon that is not a shield or claws.

    ```
    g.V.has('comic-book', 'AVF 4').inE('appeared').outV.has('weapon', T.notin, ['shield','claws']).character.order
    ```

### Load the Graph of the Gods
1. Repeat steps 1 through 5 of the Marvel graph section.
2. Load the Graph of the Gods.

    ```
    GraphOfTheGodsFactory.load(g)
    ```
3. Now you can follow the rest of the
[Titan Getting Started](http://thinkaurelius.github.io/titan/wikidoc/0.4.4/Getting-Started.html)
documentation, starting from the Global Graph Indeces section.

### Run Gremlin on Rexster
1. Repeat steps 1 through 3 of the Marvel graph section.
2. Clean up old Elasticsearch indexes.

    ```
    rm -rf /tmp/searchindex
    ```
3. Install Titan server, which includes Rexster, and follow the instructions on
the command prompt.

    ```
    src/test/resources/install-rexster.sh
    ```
Note that you will need to run rexster.sh with sudo if you choose 80 instead of 8182
as the Rexster port. You can set the Rexster port in
`src/test/resources/rexster-local.xml` when running against DynamoDB Local, and in
`src/test/resources/rexster.xml` when running against the DynamoDB endpoint in
us-east-1.
4. Test that the endpoint works from the command line. There should be no vertices.

    ```
    curl http://localhost:8182/graphs/v044/vertices | python -m json.tool
    ```
5. Navigate to http://localhost:8182/ and open a graph using the DynamoDB
Storage Backend on the Gremlin shell in the Gremlin tab.

    ```
    import com.thinkaurelius.titan.example.GraphOfTheGodsFactory; GraphOfTheGodsFactory.load(g);
    ```
6. Now you can follow the rest of the
[Titan Getting Started](http://thinkaurelius.github.io/titan/wikidoc/0.4.4/Getting-Started.html)
documentation, starting from the Global Graph Indeces section.
7. Alternatively, repeat steps 1 through 3 of this section and follow the
examples in the [Gremlin documentation](http://gremlindocs.com).
8. Alternatively, repeat steps 1 through 3 of this section and then redo steps 6
through 10 of the Marvel graph section.
9. To learn more about the Rexster REST API, visit the
[Rexster documentation](https://github.com/tinkerpop/rexster/wiki/Basic-REST-API).

### Run Gremlin on Rexster in EC2 using a CloudFormation template
The DynamoDB Storage Backend for Titan includes a CloudFormation template that
provisions an EC2 instance, installs Rexster with the DynamoDB Storage Backend
for Titan installed, and starts the Rexster REST, Doghouse and RexPro endpoints.
Requirements for running this CloudFormation template include two items. First,
you require an SSH key for EC2 instances must exist in the region you plan to
create the Rexster stack. Second, you will need the name and path of an IAM
role in the region that has S3 Read access and DynamoDB full access, the very
minimum policies required to run this CloudFormation stack. S3 read access is
required to provide the rexster.xml file to the stack in cloud-init. DynamoDB
full access is required because the DynamoDB Storage Backend for Titan can
create and delete tables, and read and write data in those tables.

Note, this cloud formation template downloads repackaged versions of the Titan zip
files available on the
[Titan downloads page](https://github.com/thinkaurelius/titan/wiki/Downloads).
We repackaged these zip files in order to include the DynamoDB Storage Backend
for Titan and its dependencies.

1. Download the latest version of the CFN template from
[GitHub](https://github.com/awslabs/dynamodb-titan-storage-backend/blob/0.4.4/dynamodb-titan-storage-backend-cfn.json).
TODO link to the template on the AWS documentation site instead
2. Navigate to the
[CloudFormation console](https://console.aws.amazon.com/cloudformation/home)
and click Create Stack.
3. On the Select Template page, name your Rexster stack and select the
CloudFormation template that you just downloaded.
4. On the Specify Parameters page, you need to specify the following:
  * EC2 Instance Type
  * The network whitelist pattern for Rexster REST, Doghouse and RexPro ports
  * The Rexster password for your stack (it will replace the password tag in
  the rexster.xml file you specify, if you have one)
  * The Rexster port, default 8182.
  * The S3 URL to your rexster.xml configuration file
  * The name of your pre-existing EC2 SSH key
  * The network whitelist for the SSH protocol. You will need to allow incoming
  connections via SSH to enable the SSH tunnels that will secure your RESTful
  API calls and use of the Doghouse.
  * The path to an IAM role that has the minimum amount of privileges to run this
  CloudFormation script and run Rexster with the DynamoDB Storage Backend for Titan.
  This role will require S3 read to get the rexster.xml file, and DynamoDB full
  access to create tables and read and write items in those tables.
5. On the Options page, click Next.
6. On the Review page, select "I acknowledge that this template might cause AWS
CloudFormation to create IAM resources." Then, click Create.
7. Create an SSH tunnel from your localhost port 8182 to the Rexster port (8182)
on the EC2 host after the stack deployment is complete. The SSH tunnel command
is one of the outputs of the CloudFormation script so you can just copy-paste
it.
8. Repeat steps 5-9 of the Rexster section above.

## Data Model
Titan-DynamoDB has a flexible data model that allows clients to select the data
model for each Titan backend table. Clients can configure tables to use either
a single-item model or a multiple-item model.

### Single-Item Model
The single-item model uses a single DynamoDB item to store all values for a
single key.  In terms of Titan backend implementations, the `key` becomes the
DynamoDB hash key, and each `column` becomes an attribute name and the column
value is stored in the respective attribute value.

This is definitely the most efficient implementation, but beware of the 400kb
limit DynamoDB imposes on items. It is best to only use this on tables you are
sure will not surpass the item size limit. Graphs with low vertex degree and
low number of items per index can take advantage of this implementation.

### Multi-Item Model
The multiple-item model uses multiple DynamoDB items to store all values for a
single key.  In terms of Titan backend implementations, the key becomes the
DynamoDB hash key, and each `column` becomes the range key in its own item.
The column values are stored in a `value` attribute.

The multiple item model is less efficient than the single-item during initial
graph loads, but it gets around the 400kb limitation. The multiple-item model
uses range Query calls instead of GetItem calls to get the necessary column
values.

## DynamoDB Specific Configuration
Each configuration option has a certain mutability level that governs whether
and how it can be modified after the database is opened for the first time. The
following listing describes the mutability levels.

1. **FIXED** - Once the database has been opened, these configuration options
cannot be changed for the entire life of the database.
2. **GLOBAL_OFFLINE** - These options can only be changed for the entire
database cluster at once when all instances are shut down.
3. **GLOBAL** - These options can only be changed globally across the entire
database cluster.
4. **MASKABLE** - These options are global but can be overwritten by a local
configuration file.
5. **LOCAL** - These options can only be provided through a local configuration
file.

Leading namespace names are shortened and sometimes spaces were inserted in long
strings to make sure the tables below is formatted correctly.

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
| `s.d.max-self-throttled-retries` | The number of retries that the backend should attempt and self-throttle. | Integer | 5 | MASKABLE |
| `s.d.initial-retry-millis` | The amount of time to initially wait (in milliseconds) when retrying self-throttled DynamoDB API calls. | Integer | 25 | MASKABLE |
| `s.d.control-plane-rate` | The rate in permits per second at which to issue DynamoDB control plane requests (CreateTable, UpdateTable, DeleteTable, ListTables, DescribeTable). | Double | 10 | MASKABLE |

### DynamoDB KeyColumnValue Store Configuration Parameters
Some configurations require specifications for each of the Titan backend
Key-Column-Value stores. Here is a list of the Titan backend
Key-Column-Value stores:
* edgestore
* edgeindex
* vertexindex
* titan_ids
* system_properties

For details about these Key-Column-Value stores, please see
[Store Mapping](https://github.com/elffersj/delftswa-aurelius-titan/blob/master/SA-doc/Mapping.md)
and
[Titan Data Model](https://github.com/thinkaurelius/titan/wiki/Titan-Data-Model).
All of these configuration parameters are in the `storage.dynamodb.stores`
(`s.d.s`) umbrella namespace subset. In the tables below these configurations
have the text `t` where the Titan table name should go.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.s.t.data-model` | SINGLE means that all the values for a given key are put into a single DynamoDB item.  A SINGLE is efficient because all the updates for a single key can be done atomically. However, the tradeoff is that DynamoDB has a 400k limit per item so it cannot hold much data. MULTI means that each 'column' is used as a range key in DynamoDB so a Titan key can span multiple items. A MULTI implementation is slightly less efficient than SINGLE because it must use DynamoDB Query rather than a direct lookup. It is HIGHLY recommended to use MULTI for edgestore, vertexindex and edgeindex unless your graph has very low max degree. | String | MULTI | FIXED |
| `s.d.s.t.capacity-read` | Define the initial read capacity for a given DynamoDB table. Make sure to replace the `s` with your actual table name. | Integer | 750 | GLOBAL |
| `s.d.s.t.capacity-write` | Define the initial write capacity for a given DynamoDB table. Make sure to replace the `s` with your actual table name. | Integer | 750 | GLOBAL |
| `s.d.s.t.read-rate` | The max number of reads per second. | Double | 750 | MASKABLE |
| `s.d.s.t.write-rate` | Used to throttle write rate of given table. The max number of writes per second. | Double | 750 | MASKABLE |
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
`storage.dynamodb.client.proxy` (`s.d.c.p`) namespace subset, and are related
to the DynamoDB SDK client proxy configuration.

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

#### DynamoDB Client Credential Configuration Parameters
All of these configuration parameters are in the `storage.dynamodb.client.credentials` (`s.d.c.c`) namespace subset, and are related to the DynamoDB SDK client credential configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.c.class-name` | Specify the fully qualified class that implements AWSCredentialsProvider or AWSCredentials. | String | `com.amazonaws.auth. BasicAWSCredentials` | MASKABLE |
| `s.d.c.c.constructor-args` | Comma separated list of strings to pass to the credentials constructor. | String | `accessKey,secretKey` | MASKABLE |

## Run all tests against DynamoDB Local on an EC2 Ubuntu or Amazon Linux AMI
1. Install dependencies. For Ubuntu:

    ```
    sudo apt-get install python-software-properties
    sudo add-apt-repository ppa:webupd8team/java
    sudo apt-get update && sudo apt-get upgrade
    echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
    echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections
    sudo apt-get install -y git maven sqlite3 libsqlite3-dev awscli unzip oracle-java7-installer
    ```
For Amazon Linux:

    ```
    sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
    sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
    sudo yum update && sudo yum upgrade
    sudo yum install -y apache-maven sqlite-devel git
    ```
2. Open a screen so that you can log out of the EC2 instance while running tests
with `screen`.
3. Start DynamoDB Local in the background with this command in a different shell
with the following command.

    ```
    mvn install && mvn test -Pstart-dynamodb-local &
    ```
4. Run the tests.

    ```
    mvn test -Pintegration-tests -Ddynamodb-partitions=15 -Ddynamodb-control-plane-rate=10000 > o
    ```
5. Exit the screen with `CTRL-A D` and logout of the EC2 instance.
6. Monitor the CPU usage of your EC2 instance in the EC2 console. These tests
may take at least 5 hours to run. When CPU usage goes to zero, that means the
tests are done.
7. Log back into the EC2 instance and resume the screen with `screen -r` to
review the test results.
8. Terminate the instance when done.
