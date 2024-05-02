package com.hashan.silva.kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I'm Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "ec2-44-201-223-243.compute-1.amazonaws.com:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Arrays.asList(topic));

        while (true) {
            logger.info("Polling");
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                logger.info("Key: "+consumerRecord.key()+", Value: "+consumerRecord.value());
                logger.info("Partition: "+consumerRecord.partition()+", Offset: "+consumerRecord.offset());
            }
        }

    }
}
