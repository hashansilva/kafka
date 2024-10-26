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

public class ProducerWithCallbackDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallbackDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I'm Kafka Producer");

        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "ec2-3-95-222-127.compute-1.amazonaws.com:9092");
        properties.setProperty("batch.size", "400");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        IntStream.range(0, 10).forEach(j -> {
            IntStream.range(0, 30).<ProducerRecord<String, String>>mapToObj(i -> new ProducerRecord<>("demo_java_partition", "message_" + i)).forEach(producerRecord -> kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }));
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        kafkaProducer.flush();

        kafkaProducer.close();

    }
}
