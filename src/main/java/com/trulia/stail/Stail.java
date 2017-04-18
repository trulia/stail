/**
 * Copyright 2017 Zillow Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.trulia.stail;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Stail {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(Stail.class);

    private static final int BATCH_SIZE = 10000; // max number of records to fetch from Kinesis in 1 request
    private static final int MAX_SHARD_THROUGHPUT = 1024 * 1000; // 1MB/s/shard  2MB/s/shard is the AWS limit, so we want to ensure we are well under that

    @Parameter(names = "--region", description = "AWS region to find the stream in", required = false)
    private String region = "us-west-2";

    @Parameter(names = "--stream", description = "Kinesis stream name to tail", required = true)
    private String stream;

    @Parameter(names = "--role", description = "role ARN to be assumed to connect to Kinesis", required = false)
    private String role = null;

    @Parameter(names = "--profile", description = "AWS profile to use for credentials", required = false)
    private String profile = null;

    @Parameter(names = "--duration", description = "how long the stream should be tailed. eg: PT15M is 15mins", required = false)
    private String duration = null;

    @Parameter(names = "--start", description = "time to start fetching records from relative to now. eg: PT15M is 15mins ago", required = false)
    private String start = null;

    private static List<Shard> getShards(AmazonKinesis client, String stream) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(stream);
        List<Shard> shards = new ArrayList<>();
        String exclusiveStartShardId = null;
        do {
            describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
            DescribeStreamResult describeStreamResult = client.describeStream(describeStreamRequest);
            shards.addAll(describeStreamResult.getStreamDescription().getShards());
            if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
                exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
            } else {
                exclusiveStartShardId = null;
            }
        } while (exclusiveStartShardId != null);

        return shards;
    }

    private static String getShardIterator(AmazonKinesis client, String stream, Shard shard, String start) {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(stream);
        getShardIteratorRequest.setShardId(shard.getShardId());

        if (!Strings.isNullOrEmpty(start)) {
            getShardIteratorRequest.setShardIteratorType(ShardIteratorType.AT_TIMESTAMP);
            getShardIteratorRequest.setTimestamp(new Date(System.currentTimeMillis() - Duration.parse(start).toMillis()));
        } else {
            getShardIteratorRequest.setShardIteratorType(ShardIteratorType.LATEST);
        }

        GetShardIteratorResult getShardIteratorResult = client.getShardIterator(getShardIteratorRequest);
        return getShardIteratorResult.getShardIterator();
    }

    public static void main(String[] args) {
        final Stail stail = new Stail();

        JCommander jct = new JCommander(stail);
        jct.setProgramName("stail");
        try {
            jct.parse(args);

            AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
            if (stail.profile != null) {
                credentialsProvider = new ProfileCredentialsProvider(stail.profile);
            }

            if (stail.role != null) {
                credentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(stail.role, "stail")
                        .withStsClient(AWSSecurityTokenServiceClientBuilder.standard()
                                .withCredentials(credentialsProvider)
                                .build())
                        .build();
            }

            AmazonKinesis client = AmazonKinesisClientBuilder.standard()
                    .withRegion(stail.region)
                    .withCredentials(credentialsProvider)
                    .build();

            // prepare the initial shard iterators at the LATEST position
            Map<Shard, String> shardIterators = new HashMap<>();
            getShards(client, stail.stream).forEach(shard -> shardIterators.put(shard, getShardIterator(client, stail.stream, shard, stail.start)));

            IRecordProcessor processor = new JSONRecordProcessor();

            Map<Shard, RateLimiter> rateLimiters = new HashMap<>();
            shardIterators.keySet().forEach(shard -> rateLimiters.put(shard, RateLimiter.create(MAX_SHARD_THROUGHPUT)));

            long end = Strings.isNullOrEmpty(stail.duration) ? Long.MAX_VALUE : System.currentTimeMillis() + Duration.parse(stail.duration).toMillis();

            while (System.currentTimeMillis() < end) {
                for (Shard shard : shardIterators.keySet()) {
                    String shardIterator = shardIterators.get(shard);

                    GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
                    getRecordsRequest.setShardIterator(shardIterator);
                    getRecordsRequest.setLimit(BATCH_SIZE);

                    try {
                        GetRecordsResult getRecordsResult = client.getRecords(getRecordsRequest);
                        List<Record> records = getRecordsResult.getRecords();
                        processor.processRecords(records, null);

                        shardIterator = getRecordsResult.getNextShardIterator();
                        shardIterators.put(shard, shardIterator);

                        if (records.size() <= 0) {
                            // nothing on the stream yet, so lets wait a bit to see if something appears
                            TimeUnit.SECONDS.sleep(1);
                        } else {
                            int bytesRead = records.stream()
                                    .map(record -> record.getData().position())
                                    .reduce((_1, _2) -> _1 + _2)
                                    .get();

                            // optionally sleep if we have hit the limit for this shard
                            rateLimiters.get(shard).acquire(bytesRead);
                        }
                    } catch (ProvisionedThroughputExceededException e) {
                        logger.warn("tripped the max throughput.  Backing off: {}", e.getMessage());
                        TimeUnit.SECONDS.sleep(6); // we tripped the max throughput.  Back off
                    }
                }
            }
        } catch (ParameterException e) {
            jct.usage();
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.exit(2);
        }
    }
}
