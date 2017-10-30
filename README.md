# Amazon DynamoDB Storage Backend for JanusGraph

> JanusGraph: Distributed Graph Database is a scalable graph database optimized for
> storing and querying graphs containing hundreds of billions of vertices and
> edges distributed across a multi-machine cluster. JanusGraph is a transactional
> database that can support thousands of concurrent users executing complex
> graph traversals in real time. -- [JanusGraph Homepage](http://janusgraph.org/)

> Amazon DynamoDB is a fast and flexible NoSQL database service for all
> applications that need consistent, single-digit millisecond latency at any
> scale. It is a fully managed database and supports both document and
> key-value data models. Its flexible data model and reliable performance make
> it a great fit for mobile, web, gaming, ad-tech, IoT, and many other
> applications.  -- [AWS DynamoDB Homepage](http://aws.amazon.com/dynamodb/)

JanusGraph + DynamoDB = Distributed Graph Database - Cluster Host Management

[![Build Status](https://travis-ci.org/awslabs/dynamodb-janusgraph-storage-backend.svg?branch=master)](https://travis-ci.org/awslabs/dynamodb-janusgraph-storage-backend)

## Features
The following is a list of features of the Amazon DynamoDB Storage Backend for
JanusGraph.
* AWS managed authentication and authorization.
* Configure table prefix to allow multiple graphs to be stored in a single
account in the same region.
* Full graph traversals with rate limited table scans.
* Flexible data model allows configuration between single-item and
multiple-item model based on graph size and utilization.
* Test graph locally with DynamoDB Local.
* Integrated with JanusGraph metrics.
* JanusGraph 0.1.1 and TinkerPop 3.2.3 compatibility.
* Upgrade compatibility from Titan 1.0.0.

## Getting Started
This example populates a JanusGraph database backed by DynamoDB Local using
the
[Marvel Universe Social Graph](https://aws.amazon.com/datasets/5621954952932508).
The graph has a vertex per comic book character with an edge to each of the
comic books in which they appeared.

### Load a subset of the Marvel Universe Social Graph
1. Install the prerequisites (Git, JDK 1.8, Maven, Docker, wget, gpg) of this tutorial.
The command below uses a
[convenience script for Amazon Linux](https://raw.githubusercontent.com/awslabs/dynamodb-janusgraph-storage-backend/master/src/test/resources/install-reqs.sh)
on EC2 instances to install Git, Open JDK 1.8, Maven, Docker and Docker Compose.
It adds the ec2-user to the docker group so that you can
[execute Docker commands without using sudo](http://docs.aws.amazon.com/AmazonECS/latest/developerguide/docker-basics.html).
Log out and back in to effect changes on ec2-user.

    ```bash
    curl https://raw.githubusercontent.com/awslabs/dynamodb-janusgraph-storage-backend/master/src/test/resources/install-reqs.sh | bash
    exit
    ```
2. Clone the repository and change directories.

    ```bash
    git clone https://github.com/awslabs/dynamodb-janusgraph-storage-backend.git && cd dynamodb-janusgraph-storage-backend
    ```
3. Use Docker and Docker Compose to bake DynamoDB Local into a container and start Gremlin Server with the DynamoDB
Storage Backend for JanusGraph installed.

    ```bash
    docker build -t awslabs/dynamodblocal ./src/test/resources/dynamodb-local-docker \
    && src/test/resources/install-gremlin-server.sh \
    && cp server/dynamodb-janusgraph-storage-backend-*.zip src/test/resources/dynamodb-janusgraph-docker \
    && mvn docker:build -Pdynamodb-janusgraph-docker \
    && docker-compose -f src/test/resources/docker-compose.yml up -d \
    && docker exec -i -t dynamodb-janusgraph /var/jg/bin/gremlin.sh
    ```
4. After the Gremlin shell starts, set it up to execute commands remotely.

    ```groovy
    :remote connect tinkerpop.server conf/remote.yaml session
    :remote console
    ```
5. Load the first 100 lines of the Marvel graph using the Gremlin shell.

    ```groovy
    com.amazon.janusgraph.example.MarvelGraphFactory.load(graph, 100, false)
    ```
6. Print the characters and the comic-books they appeared in where the
characters had a weapon that was a shield or claws.

    ```groovy
    g.V().has('weapon', within('shield','claws')).as('weapon', 'character', 'book').select('weapon', 'character','book').by('weapon').by('character').by(__.out('appeared').values('comic-book'))
    ```
7. Print the characters and the comic-books they appeared in where the
characters had a weapon that was not a shield or claws.

    ```groovy
    g.V().has('weapon').has('weapon', without('shield','claws')).as('weapon', 'character', 'book').select('weapon', 'character','book').by('weapon').by('character').by(__.out('appeared').values('comic-book'))
    ```
8. Print a sorted list of the characters that appear in comic-book AVF 4.

    ```groovy
    g.V().has('comic-book', 'AVF 4').in('appeared').values('character').order()
    ```
9. Print a sorted list of the characters that appear in comic-book AVF 4 that
have a weapon that is not a shield or claws.

    ```groovy
    g.V().has('comic-book', 'AVF 4').in('appeared').has('weapon', without('shield','claws')).values('character').order()
    ```
10. Exit remote mode and Control-C to quit.

    ```groovy
    :remote console
    ```
11. Clean up the composed Docker containers.

    ```bash
    docker-compose -f src/test/resources/docker-compose.yml stop
    ```

### Load the Graph of the Gods
1. Repeat steps 3 and 4 of the Marvel graph section, cleaning up the server directory beforehand with `rm -rf server`.
2. Load the Graph of the Gods.

    ```groovy
    GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true)
    ```
3. Now you can follow the rest of the
[JanusGraph Getting Started](http://docs.janusgraph.org/0.1.0/getting-started.html#_global_graph_indices)
documentation, starting from the Global Graph Indeces section. See the
`scriptEngines/gremlin-groovy/scripts` list element in the Gremlin Server YAML
file for more information about what is in scope in the remote environment.
4. Alternatively, repeat steps 1 through 8 of the Marvel graph section and
follow the examples in the
[TinkerPop documentation](http://tinkerpop.apache.org/docs/3.2.3/reference/#_mutating_the_graph).
Skip the `TinkerGraph.open()` step as the remote execution environment already has a
`graph` variable set up. TinkerPop have
[other tutorials](http://tinkerpop.apache.org/docs/3.2.3/#tutorials) available as well.

### Run Gremlin on Gremlin Server in EC2 using CloudFormation templates
The DynamoDB Storage Backend for JanusGraph includes CloudFormation templates that
creates a VPC, an EC2 instance in the VPC, installs Gremlin Server with the
DynamoDB Storage Backend for JanusGraph installed, and starts the Gremlin Server
Websocket endpoint. Also included are templates that create the graph's DynamoDB tables.
The Network ACL of the VPC includes just enough access to allow:

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
 - You require permission to call the ec2:DescribeKeyPairs API when creating a stack
   from the AWS console.
 - You need to have created an IAM role in the region that has S3 Read access
   and DynamoDB full access, the very minimum policies required to run this
   CloudFormation stack. S3 read access is required to provide the dynamodb.properties
   file to the stack in cloud-init. DynamoDB full access is required because the
   DynamoDB Storage Backend for JanusGraph can create and delete tables, and read and
   write data in those tables.

Note, this cloud formation template downloads
the JanusGraph zip files available on the
[JanusGraph downloads page](https://github.com/JanusGraph/janusgraph/releases).
The CloudFormation template downloads these packages and builds and adds the
DynamoDB Storage Backend for JanusGraph with its dependencies.

#### CloudFormation Template table
Below you can find a list of CloudFormation templates discussed in this document,
and links to launch each stack in CloudFormation and to view the stack in the designer.

| Template name              | Description                                                | View |
|----------------------------|------------------------------------------------------------|------|
| Single-Item Model Tables   | Set up six graph tables with the single item data model.   | [View](https://raw.githubusercontent.com/awslabs/dynamodb-titan-storage-backend/master/dynamodb-janusgraph-tables-single.yaml) |
| Multiple-Item Model Tables | Set up six graph tables with the multiple item data model. | [View](https://raw.githubusercontent.com/awslabs/dynamodb-titan-storage-backend/master/dynamodb-janusgraph-tables-multiple.yaml) |
| Gremlin Server on DynamoDB | The HTTP user agent header to send with all requests.      | [View](https://raw.githubusercontent.com/awslabs/dynamodb-titan-storage-backend/master/dynamodb-janusgraph-storage-backend-cfn.yaml) |

#### Instructions to Launch CloudFormation Stacks
1. Choose between the single and multiple item data models and create your graph tables
with the corresponding CloudFormation template above by downloading it and passing it
to the CloudFormation console. Note, the configuration provided in
`src/test/resources/dynamodb.properties` assumes that you
will deploy the stack in us-west-2 and that you will use the multiple item model.
2. Inspect the latest version of the Gremlin Server on DynamoDB stack in the third row above.
3. Download the template from the third row to your computer and use it to create the Gremlin
Server on DynamoDB stack.
4. On the Select Template page, name your Gremlin Server stack and select the
CloudFormation template that you just downloaded.
5. On the Specify Parameters page, you need to specify the following:
  * EC2 Instance Type
  * The Gremlin Server port, default 8182.
  * The S3 URL to your dynamodb.properties configuration file
  * The name of your pre-existing EC2 SSH key. Be sure to `chmod 400` on your key as
  EC2 instance will reject connections if permissions on the key are
  [too open](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/TroubleshootingInstancesConnecting.html#w1ab1c28c17c21).
  * The network whitelist for the SSH protocol. You will need to allow incoming
  connections via SSH to enable the SSH tunnels that will secure Websockets connections
  to Gremlin Server.
  * The path to an IAM role that has the minimum amount of privileges to run this
  CloudFormation script and run Gremlin Server with the DynamoDB Storage Backend for
  JanusGraph. This role will require S3 read to get the dynamodb.properties file, and DynamoDB full
  access to create tables and read and write items in those tables. This IAM role needs to be created with
  a STS trust relationship including `ec2.amazonaws.com` as an identity provider. The easiest way to do
  this is to [create a new role on the IAM console](https://console.aws.amazon.com/iam/home?region=us-west-2#/roles)
  and from the AWS Service Role list in the accordion, select Amazon EC2, and add the AmazonDynamoDBFullAccess
  and AmazonS3ReadOnlyAccess managed policies.
6. On the Options page, click Next.
7. On the Review page, select "I acknowledge that this template might cause AWS
CloudFormation to create IAM resources." Then, click Create.
8. Start the Gremlin console on the host through SSH. You can just copy paste the `GremlinShell` output of the 
CloudFormation template and run it on your command line.
9. Repeat steps 4 and onwards of the Marvel graph section above.

## Data Model
The Amazon DynamoDB Storage Backend for JanusGraph has a flexible data model that
allows clients to select the data model for each JanusGraph backend table. Clients
can configure tables to use either a single-item model or a multiple-item model.

### Single-Item Model
The single-item model uses a single DynamoDB item to store all values for a
single key.  In terms of JanusGraph backend implementations, the key becomes the
DynamoDB hash key, and each column becomes an attribute name and the column
value is stored in the respective attribute value.

This is definitely the most efficient implementation, but beware of the 400kb
limit DynamoDB imposes on items. It is best to only use this on tables you are
sure will not surpass the item size limit. Graphs with low vertex degree and
low number of items per index can take advantage of this implementation.

### Multiple-Item Model
The multiple-item model uses multiple DynamoDB items to store all values for a
single key.  In terms of JanusGraph backend implementations, the key becomes the
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
| `s.backend` | The primary persistence provider used by JanusGraph. To use DynamoDB you must set this to `com.amazon.janusgraph.diskstorage. dynamodb.DynamoDBStoreManager` | String |  | LOCAL |
| `s.d.prefix` | A prefix to put before the JanusGraph table name. This allows clients to have multiple graphs in the same AWS DynamoDB account in the same region. | String | jg | LOCAL |
| `s.d.metrics-prefix` | Prefix on the codahale metric names emitted by DynamoDBDelegate. | String | d | LOCAL |
| `s.d.force-consistent-read` | This feature sets the force consistent read property on DynamoDB calls. | Boolean | true | LOCAL |
| `s.d.enable-parallel-scan` | This feature changes the scan behavior from a sequential scan (with consistent key order) to a segmented, parallel scan. Enabling this feature will make full graph scans faster, but it may cause this backend to be incompatible with Titan's OLAP library. | Boolean | false | LOCAL |
| `s.d.max-self-throttled-retries` | The number of retries that the backend should attempt and self-throttle. | Integer | 60 | LOCAL |
| `s.d.initial-retry-millis` | The amount of time to initially wait (in milliseconds) when retrying self-throttled DynamoDB API calls. | Integer | 25 | LOCAL |
| `s.d.control-plane-rate` | The rate in permits per second at which to issue DynamoDB control plane requests (CreateTable, UpdateTable, DeleteTable, ListTables, DescribeTable). | Double | 10 | LOCAL |
| `s.d.native-locking` | Set this to false if you need to use JanusGraph's locking mechanism for remote lock expiry. | Boolean | true | LOCAL |
| `s.d.use-titan-ids` | Set this to true if you are migrating from Titan to JanusGraph so that you do not have to copy your titan_ids table. | Boolean | false | LOCAL |

### DynamoDB KeyColumnValue Store Configuration Parameters
Some configurations require specifications for each of the JanusGraph backend
Key-Column-Value stores. Here is a list of the default JanusGraph backend
Key-Column-Value stores:
* edgestore
* graphindex
* janusgraph_ids (this used to be called titan_ids in Titan)
* system_properties
* systemlog
* txlog

Any store you define in the umbrella `storage.dynamodb.stores.*` namespace that starts
with `ulog_` will be used for user-defined transaction logs.

Again, if you opt out of storage-native locking with the
`storage.dynamodb.native-locking = false` configuration, you will need to configure the
data model, initial capacity and rate limiters for the three following stores:
* edgestore_lock_
* graphindex_lock_
* system_properties_lock_

You can configure the initial read and write capacity, rate limits, scan limits and
data model for each KCV graph store. You can always scale up and down the read and
write capacity of your tables in the DynamoDB console. If you have a write once,
read many workload, or you are running a bulk data load, it is useful to adjust the
capacity of edgestore and graphindex tables as necessary in the DynamoDB console,
and decreasing the allocated capacity and rate limiters afterwards.

For details about these Key-Column-Value stores, please see
[Store Mapping](https://github.com/BillBaird/delftswa-aurelius-titan/blob/master/SA-doc/Mapping.md)
and
[JanusGraph Data Model](http://docs.janusgraph.org/0.1.0/data-model.html).
All of these configuration parameters are in the `storage.dynamodb.stores`
(`s.d.s`) umbrella namespace subset. In the tables below these configurations
have the text `t` where the JanusGraph store name should go.

When upgrading from Titan 1.0.0, you will need to set the
[ids.store-name configuration](http://docs.janusgraph.org/latest/config-ref.html#_ids)
to `titan_ids` to avoid re-using id ranges that are already assigned.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.s.t.data-model` | SINGLE means that all the values for a given key are put into a single DynamoDB item.  A SINGLE is efficient because all the updates for a single key can be done atomically. However, the tradeoff is that DynamoDB has a 400k limit per item so it cannot hold much data. MULTI means that each 'column' is used as a range key in DynamoDB so a key can span multiple items. A MULTI implementation is slightly less efficient than SINGLE because it must use DynamoDB Query rather than a direct lookup. It is HIGHLY recommended to use MULTI for edgestore and graphindex unless your graph has very low max degree.| String | MULTI | FIXED |
| `s.d.s.t.initial-capacity-read` | Define the initial read capacity for a given DynamoDB table. Make sure to replace the `s` with your actual table name. | Integer | 4 | LOCAL |
| `s.d.s.t.initial-capacity-write` | Define the initial write capacity for a given DynamoDB table. Make sure to replace the `s` with your actual table name. | Integer | 4 | LOCAL |
| `s.d.s.t.read-rate` | The max number of reads per second. | Double | 4 | LOCAL |
| `s.d.s.t.write-rate` | Used to throttle write rate of given table. The max number of writes per second. | Double | 4 | LOCAL |
| `s.d.s.t.scan-limit` | The maximum number of items to evaluate (not necessarily the number of matching items). If DynamoDB processes the number of items up to the limit while processing the results, it stops the operation and returns the matching values up to that point, and a key in LastEvaluatedKey to apply in a subsequent operation, so that you can pick up where you left off. Also, if the processed data set size exceeds 1 MB before DynamoDB reaches this limit, it stops the operation and returns the matching values up to the limit, and a key in LastEvaluatedKey to apply in a subsequent operation to continue the operation. | Integer | 10000 | LOCAL |

### DynamoDB Client Configuration Parameters
All of these configuration parameters are in the `storage.dynamodb.client`
(`s.d.c`) namespace subset, and are related to the DynamoDB SDK client
configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.connection-timeout` | The amount of time to wait (in milliseconds) when initially establishing a connection before giving up and timing out. | Integer | 60000 | LOCAL |
| `s.d.c.connection-ttl` | The expiration time (in milliseconds) for a connection in the connection pool. | Integer | 60000 | LOCAL |
| `s.d.c.connection-max` |  The maximum number of allowed open HTTP connections.| Integer | 10 | LOCAL |
| `s.d.c.retry-error-max` |  The maximum number of retry attempts for failed retryable requests (ex: 5xx error responses from services).| Integer | 0 | LOCAL |
| `s.d.c.use-gzip` |   Sets whether gzip compression should be used. | Boolean | false | LOCAL |
| `s.d.c.use-reaper` |  Sets whether the IdleConnectionReaper is to be started as a daemon thread. | Boolean | true | LOCAL |
| `s.d.c.user-agent` | The HTTP user agent header to send with all requests.| String |  | LOCAL |
| `s.d.c.endpoint` | Sets the service endpoint to use for connecting to DynamoDB. | String | | LOCAL |
| `s.d.c.signing-region` | Sets the signing region to use for signing requests to DynamoDB. Required. | String | | LOCAL |

#### DynamoDB Client Proxy Configuration Parameters
All of these configuration parameters are in the
`storage.dynamodb.client.proxy` (`s.d.c.p`) namespace subset, and are related to
the DynamoDB SDK client proxy configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.p.domain` | The optional Windows domain name for configuration an NTLM proxy.| String | | LOCAL |
| `s.d.c.p.workstation` | The optional Windows workstation name for configuring NTLM proxy support.| String | | LOCAL |
| `s.d.c.p.host` | The optional proxy host the client will connect through.| String | | LOCAL |
| `s.d.c.p.port` | The optional proxy port the client will connect through.| String | | LOCAL |
| `s.d.c.p.username` | The optional proxy user name to use if connecting through a proxy.| String | | LOCAL |
| `s.d.c.p.password` |  The optional proxy password to use when connecting through a proxy.| String | | LOCAL |

#### DynamoDB Client Socket Configuration Parameters
All of these configuration parameters are in the `storage.dynamodb.client.socket`
(`s.d.c.s`) namespace subset, and are related to the DynamoDB SDK client socket
configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.s.buffer-send-hint` | The optional size hints (in bytes) for the low level TCP send and receive buffers.| Integer | 1048576 | LOCAL |
| `s.d.c.s.buffer-recv-hint` | The optional size hints (in bytes) for the low level TCP send and receive buffers.| Integer | 1048576 | LOCAL |
| `s.d.c.s.timeout` | The amount of time to wait (in milliseconds) for data to be transfered over an established, open connection before the connection times out and is closed.| Long | 50000 | LOCAL |
| `s.d.c.s.tcp-keep-alive` | Sets whether or not to enable TCP KeepAlive support at the socket level. Not used at the moment. | Boolean |  | LOCAL |

#### DynamoDB Client Executor Configuration Parameters
All of these configuration parameters are in the `storage.dynamodb.client.executor`
(`s.d.c.e`) namespace subset, and are related to the DynamoDB SDK client
executor / thread-pool configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.e.core-pool-size` |  The core number of threads for the DynamoDB async client. | Integer | 25| LOCAL |
| `s.d.c.e.max-pool-size` | The maximum allowed number of threads for the DynamoDB async client. | Integer | 50 | LOCAL |
| `s.d.c.e.keep-alive` | The time limit for which threads may remain idle before being terminated for the DynamoDB async client.  | Integer | | LOCAL |
| `s.d.c.e.max-queue-length` | The maximum size of the executor queue before requests start getting run in the caller.  | Integer | 1024 | LOCAL |
| `s.d.c.e.max-concurrent-operations` | The expected number of threads expected to be using a single JanusGraph instance. Used to allocate threads to batch operations. | Integer | 1 | LOCAL |

#### DynamoDB Client Credential Configuration Parameters
All of these configuration parameters are in the `storage.dynamodb.client.credentials`
(`s.d.c.c`) namespace subset, and are related to the DynamoDB SDK client
credential configuration.

| Name            | Description | Datatype | Default Value | Mutability |
|-----------------|-------------|----------|---------------|------------|
| `s.d.c.c.class-name` | Specify the fully qualified class that implements AWSCredentialsProvider or AWSCredentials. | String | `com.amazonaws.auth. BasicAWSCredentials` | LOCAL |
| `s.d.c.c.constructor-args` | Comma separated list of strings to pass to the credentials constructor. | String | `accessKey,secretKey` | LOCAL |

## Upgrading from Titan 1.0.0
Earlier versions of this software supported Titan 1.0.0. This software supports upgrading from
the DynamoDB Storage Backend for Titan 1.0.0 by following the steps to update your configuration below.
1. Set the JanusGraph configuration option
[`ids.store-name=titan_ids`](http://docs.janusgraph.org/latest/config-ref.html#_ids). This allows you to reuse your
`titan_ids` table.
2. Update the classpath to the DynamoDB Storage Backend to use the latest package name,
`storage.backend=com.amazon.janusgraph.diskstorage.dynamodb.DynamoDBStoreManager` .

## Run all tests against DynamoDB Local on an EC2 Amazon Linux AMI
1. Install dependencies. For Amazon Linux:

    ```bash
    sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo \
      -O /etc/yum.repos.d/epel-apache-maven.repo
    sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
    sudo yum update -y && sudo yum upgrade -y
    sudo yum install -y apache-maven sqlite-devel git java-1.8.0-openjdk-devel
    sudo alternatives --set java /usr/lib/jvm/jre-1.8.0-openjdk.x86_64/bin/java
    sudo alternatives --set javac /usr/lib/jvm/java-1.8.0-openjdk.x86_64/bin/javac
    git clone https://github.com/awslabs/dynamodb-janusgraph-storage-backend.git
    cd dynamodb-janusgraph-storage-backend && mvn install
    ```
2. Open a screen so that you can log out of the EC2 instance while running tests with `screen`.
3. Run the single-item data model tests.

    ```bash
    mvn verify -P integration-tests \
    -Dexclude.category=com.amazon.janusgraph.testcategory.MultipleItemTestCategory \
    -Dinclude.category="**/*.java" > o 2>&1
    ```
4. Run the multiple-item data model tests.

    ```bash
    mvn verify -P integration-tests \
    -Dexclude.category=com.amazon.janusgraph.testcategory.SingleItemTestCategory \
    -Dinclude.category="**/*.java" > o 2>&1
    ```
5. Run other miscellaneous tests.

    ```bash
    mvn verify -P integration-tests -Dinclude.category="**/*.java" \
        -Dgroups=com.amazon.janusgraph.testcategory.IsolateRemainingTestsCategory > o 2>&1
    ```
6. Exit the screen with `CTRL-A D` and logout of the EC2 instance.
7. Monitor the CPU usage of your EC2 instance in the EC2 console. The single-item tests
may take at least 1 hour and the multiple-item tests may take at least 2 hours to run.
When CPU usage goes to zero, that means the tests are done.
8. Log back into the EC2 instance and resume the screen with `screen -r` to
review the test results.

    ```bash
    cd target/surefire-reports && grep testcase *.xml | grep -v "\/"
    ```
9. Terminate the instance when done.
