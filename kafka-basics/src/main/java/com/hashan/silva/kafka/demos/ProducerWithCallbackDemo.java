package com.hashan.silva.kafka.demos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbackDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallbackDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I'm Kafka Producer");

        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "ec2-3-95-222-127.compute-1.amazonaws.com:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "second message");

        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();

        kafkaProducer.close();

    }
}
