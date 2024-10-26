package com.hashan.silva.kafka.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerWithKeys {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I'm Kafka Producer");

        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "ec2-44-201-223-243.compute-1.amazonaws.com:9092");
        properties.setProperty("batch.size", "400");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);


        IntStream.range(0, 30).forEach(i -> {
            String topic = "demo_java";
            String key = "id_" + i;
            String value = "hello world " + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata \n" +
                                "Key: " + key + "\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        });

        kafkaProducer.flush();

        kafkaProducer.close();

    }
}
