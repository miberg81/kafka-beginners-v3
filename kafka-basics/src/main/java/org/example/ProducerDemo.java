package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log =
            LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        Properties properties = new Properties();
        // connect to kafka on localhost
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to cloud kafka cluster
        properties.setProperty("bootstrap.servers", "pkc-gy65n.europe-west12.gcp.confluent.cloud:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='YPH7PX6RYT6OBX7G' password='TspUbYo+6lgUFlvRS4HAeinXRmB1pdbln5wwbH1Pf2gxE13aj9ZJGqvjUTgX4T83';");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // create producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create hte Producer
        // <String,String> matches the types of key and value
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        // asynchronous operation!(will not immediately  happen)
        producer.send(producerRecord);

        // tell producer to send all data and block until done (synchronous operation)
        //producer.flush();

        // flush and close producer
        producer.close(); // this operation will do flush() internally
    }
}
