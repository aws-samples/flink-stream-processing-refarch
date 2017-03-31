/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *    http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.flink.refarch.utils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.base.Supplier;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ElasticsearchJestSink<T> extends RichSinkFunction<T> {
    private JestClient client;
    private List<Index> batch;

    private final String indexName;
    private final String documentType;
    private final int batchSize;
    private final long maxBufferTime;
    private final Map<String, String> userConfig;

    private long lastBufferFlush;

    private static final String ES_SERVICE_NAME = "es";

    public ElasticsearchJestSink(Map<String, String> config, String indexName, String documentType) {
        this(config, indexName, documentType, 500, 5000);
    }

    public ElasticsearchJestSink(Map<String, String> config, String indexName, String documentType, int batchSize, long maxBufferTime) {
        this.userConfig = config;
        this.indexName = indexName;
        this.documentType = documentType;
        this.batchSize = batchSize;
        this.maxBufferTime = maxBufferTime;

        this.lastBufferFlush = System.currentTimeMillis();
    }

    @Override
    public void invoke(T document)  {
        batch.add(new Index.Builder(document).index(indexName).type(documentType).build());

        if (batch.size() >= batchSize || System.currentTimeMillis()-lastBufferFlush >= maxBufferTime) {
            Bulk.Builder bulkIndexBuilder = new Bulk.Builder();

            batch.forEach(bulkIndexBuilder::addAction);

            try {
                client.execute(bulkIndexBuilder.build());
            } catch (IOException e) {
                return;
            }

            batch.clear();
            lastBufferFlush = System.currentTimeMillis();
        }
    }

    @Override
    public void open(Configuration configuration) {
        ParameterTool params = ParameterTool.fromMap(userConfig);

        final Supplier<LocalDateTime> clock = () -> LocalDateTime.now(ZoneOffset.UTC);
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        final AWSSigner awsSigner = new AWSSigner(credentialsProvider, params.getRequired("region"), ES_SERVICE_NAME, clock);

        final AWSSigningRequestInterceptor requestInterceptor = new AWSSigningRequestInterceptor(awsSigner);

        final JestClientFactory factory = new JestClientFactory() {
            @Override
            protected HttpClientBuilder configureHttpClient(HttpClientBuilder builder) {
                builder.addInterceptorLast(requestInterceptor);
                return builder;
            }

            @Override
            protected HttpAsyncClientBuilder configureHttpClient(HttpAsyncClientBuilder builder) {
                builder.addInterceptorLast(requestInterceptor);
                return builder;
            }
        };

        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(params.getRequired("es-endpoint"))
                .multiThreaded(true)
                .build());

        client = factory.getObject();
        batch = new ArrayList<>(batchSize);
    }
}
