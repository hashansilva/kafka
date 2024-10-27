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

package com.hashan.silva.kafka.util;

public class Constants {

    public static final String INDEX_REQUEST = "wikimedia";
    public static final String JAVAX_SSL_TRUSTSTORE = "javax.net.ssl.trustStore";
    public static final String PATH_SSL_TRUSTSTORE = "D:\\kafka\\certs\\kafka-consumer.keystore";
    public static final String JAVAX_SSL_TRUSTSTORE_PASS = "javax.net.ssl.trustStorePassword";
    public static final String SSL_TRUSTSTORE_PASS = "kafka-consumer";
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String GROUP_ID_CONFIG = "consumer-opensearch";
    public static final String AUTO_OFFSET_RESET_CONFIG = "latest";
    public static final String KAFKA_TOPIC = "wikimedia.recentchange";
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "false";
}
