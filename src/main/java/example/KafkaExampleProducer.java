package example;

import example.model.DataRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.errors.TopicExistsException;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Collections;

/**
 * Simple Kafka producer
 * mvn exec:java -Dexec.mainClass="src.main.java.de.wkuch.app.KafkaExampleProducer" \
 *         -Dexec.args="$PWD/java.config test1"
 */


public class KafkaExampleProducer {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        if (args.length != 2) {
            System.out.println("Please provide command line arguments: configPath topic");
            System.exit(1);
        }

        final Properties props = loadConfig(args[0]);

        // Create topic if needed
        final String topic = args[1];
        createTopic(topic, 1, 1, props);

        // Add additional properties.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Class.forName("io.confluent.kafka.serializers.KafkaJsonSerializer"));

        Producer<String, DataRecord> producer = new KafkaProducer<String, DataRecord>(props);

        final long numMsgs = 10L;

        for (long i = 0L; i < numMsgs; i++) {
            String key = "schwarz";
            DataRecord record = new DataRecord(i);

            System.out.printf("Producing record: %s\t%s%n", key, record);

            producer.send(new ProducerRecord<String, DataRecord>(topic, key, record), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.printf("Produces record to topic %s partition [%d] @ offset %d%n", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                    }
                }
            });
        }

        producer.flush();

        System.out.printf("%d Messages were produced to topic %s%n", numMsgs, topic);

        producer.close();

    }

    public static void createTopic(final String topic,
                                   final int partitions,
                                   final int replication,
                                   final Properties cloudConfig) {
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
        try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}
