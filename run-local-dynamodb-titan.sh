mvn install
echo 'compiled dynamodb-titan-storage-backend using maven'
mvn test -Pstart-dynamodb-local &
echo 'started dyanmodb local'
cd server/dynamodb-titan100-storage-backend-1.0.0-hadoop1
bin/gremlin-server.sh ${PWD}/conf/gremlin-server/gremlin-server-local.yaml &
echo 'started gremlin server'
