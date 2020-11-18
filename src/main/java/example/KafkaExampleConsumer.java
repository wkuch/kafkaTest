package example;

import example.model.DataRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import io.confluent.kafka.serializers.*;



import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

public class KafkaExampleConsumer {
    public static void main(String[] args) throws IOException{
        if (args.length != 2) {
            System.out.println("Please provide command line arguments: configPath topic");
            System.exit(1);
        }

        final String topic = args[1];

        final Properties props = loadConfig(args[0]);
        // Add additional properties.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, DataRecord.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, DataRecord> consumer = new KafkaConsumer<String, DataRecord>(props);
        consumer.subscribe(Arrays.asList(topic));

        Long totalCount = 0L;

        try{
            while (true){
                ConsumerRecords<String, DataRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, DataRecord> record : records) {
                    String key = record.key();
                    DataRecord value = record.value();
                    totalCount += value.getCount();
                    System.out.printf("Consumed record with key %s and value %s, and updated total count to %d%n", key, value, totalCount);
                }
            }
        } finally {
            consumer.close();
        }

    }

    public static Properties loadConfig(String configFile) throws IOException {
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
