package org.example.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys3 {
    private static final Logger log =
            LoggerFactory.getLogger(ProducerDemoWithKeys3.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm a kafka Producer");
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

        // create the Producer
        // <String,String> matches the types of key and value
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // The same keys will go to the same partition!
        for (int j = 0; j < 2; j++) {
            // single batch
            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world " + i;

                // create a Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // asynchronous operation!(will not immediately  happen)
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Key: " + key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // tell producer to send all data and block until done (synchronous operation)
        producer.flush();

        // flush and close producer
        producer.close(); // this operation will do flush() internally
    }
}
