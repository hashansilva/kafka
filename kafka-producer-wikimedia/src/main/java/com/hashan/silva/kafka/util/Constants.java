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
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String KAFKA_TOPIC = "wikimedia.recentchange";
    public static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    public static final String LINGER_MS_CONFIG = "20";
    public static final String BATCH_SIZE_CONFIG = Integer.toString(32*1024);
    public static final String COMPRESSION_TYPE_CONFIG = "snappy";
}
