## to run locally (on linux)
### run zookeeper with docker
`docker run --name zookeeper -p 2181:2181  zookeeper`

### run one kafka broker with docker and connect to zookeeper
`docker run --name kafka -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=hostname:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://hostname:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka`

remember to replace `hostname` with the actual hostname
 
### run example producer/consumer
1. `mvn clean install`
2. `mvn exec:java -Dexec.mainClass="example.KafkaEcampleProducer" \
    -Dexec.args="$PWD/java.config test1"`
3. `mvn exec:java -Dexec.mainClass="example.KafkaEcampleConsumer" \
    -Dexec.args="$PWD/java.config test1"`
    
