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

package com.hashan.silva.kafka.consumer;

import com.hashan.silva.kafka.util.Constants;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OpenSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws RuntimeException {

        RestHighLevelClient openSearchClient = createRestHighLevelClient();

        try (openSearchClient) {

            boolean isIndexExists = openSearchClient.indices().exists(new GetIndexRequest(Constants.INDEX_REQUEST), RequestOptions.DEFAULT);
            if (!isIndexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(Constants.INDEX_REQUEST);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("Index has been created");
            } else {
                logger.info("Index already exists");
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    /**
     * Create Rest High Level Client
     *
     * @return
     */
    private static RestHighLevelClient createRestHighLevelClient() {

        System.setProperty(Constants.JAVAX_SSL_TRUSTSTORE, Constants.PATH_SSL_TRUSTSTORE);
        System.setProperty(Constants.JAVAX_SSL_TRUSTSTORE_PASS, Constants.SSL_TRUSTSTORE_PASS);

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("admin", "Shermal@1989"));

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        return new RestHighLevelClient(restClientBuilder);

    }
}
