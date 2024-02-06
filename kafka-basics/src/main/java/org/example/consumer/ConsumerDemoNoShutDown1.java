package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoNoShutDown1 {
    private static final Logger log =
            LoggerFactory.getLogger(ConsumerDemoNoShutDown1.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I'm a kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create Consumer properties
        Properties properties = new Properties();

        // connect to kafka on localhost
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to cloud kafka cluster
        properties.setProperty("bootstrap.servers", "pkc-gy65n.europe-west12.gcp.confluent.cloud:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='YPH7PX6RYT6OBX7G' password='TspUbYo+6lgUFlvRS4HAeinXRmB1pdbln5wwbH1Pf2gxE13aj9ZJGqvjUTgX4T83';");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        /*
          "none" - if we don't have any existing consumer group we fail
           must set consumer group before stating application
          "earliest" - read from beginning of my topic (read entire topic history)
          "latest" - read only latest messages (from now on)
          This setting works on first run (then consumer first joins group)
          If consumer restarts, then it re-joins the group and reads
           from last committed offsets(not from beginning)
         */
        properties.setProperty("auto.offset.reset", "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to the topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data. consumer will receive batches of messages from all partitions
        while (true) {
            log.info("Polling");
            // if there is data - it is sent immediately and there is no wait
            // but if no data - we wait a second to get Kafka opportunity to bring data
            // This is to prevent load on Kafka
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record :records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }

    }
}
