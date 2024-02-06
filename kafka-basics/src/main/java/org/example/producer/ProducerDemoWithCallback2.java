package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback2 {
    private static final Logger log =
            LoggerFactory.getLogger(ProducerDemoWithCallback2.class.getSimpleName());
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

        properties.setProperty("batch.size", "400");

        // this partitioner will all partitions one by one ...This setting not recommended.
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // create the Producer
        // <String,String> matches the types of key and value
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // each time run a batch and sleep
        for (int j = 0; j < 10; j++) {
            // SINGLE 30 MESSAGES BATCH
            // we are only specifying the value(message), but not the key
            // this is why a default uniform(sticky) partitioner will be used (partitioner.class=null in log)
            for (int i = 0; i < 30; i++) {
                // create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world" + i);

                // asynchronous operation!(will not immediately  happen)
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() +"\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp:" + metadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        // tell producer to send all data and block until done (synchronous operation)
        producer.flush();

        // flush and close producer
        producer.close(); // this operation will do flush() internally
    }
}
