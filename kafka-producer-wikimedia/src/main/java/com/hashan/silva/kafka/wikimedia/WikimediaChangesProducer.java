/*
 * Copyright (c) 2024. Hashan Silva
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 */

package com.hashan.silva.kafka.wikimedia;

import com.hashan.silva.kafka.handler.WikimediaChangeHandler;
import com.hashan.silva.kafka.util.Constants;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hashan.silva.kafka.util.Constants.BOOTSTRAP_SERVERS;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //higher throughput properties
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, Constants.LINGER_MS_CONFIG);
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Constants.BATCH_SIZE_CONFIG);
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, Constants.COMPRESSION_TYPE_CONFIG);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        EventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, Constants.KAFKA_TOPIC);

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(Constants.WIKIMEDIA_URL));
        EventSource eventSource = builder.build();

        eventSource.start();

        TimeUnit.MINUTES.sleep(10);

    }
}
