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
 *
 */

package com.amazonaws.flink.refarch.utils;

import com.amazonaws.flink.refarch.events.TripEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;


public class TaxiEventReader implements Iterator<TripEvent>
{
    private static final Logger LOG = LoggerFactory.getLogger(TaxiEventReader.class);

    private final AmazonS3 s3;
    private final Iterator<S3ObjectSummary> s3Objects;
    private S3Object s3Object;
    private BufferedReader objectStream;

    private TripEvent next;
    private boolean hasNext = true;

    public TaxiEventReader(AmazonS3 s3, String bucketName, String prefix) {
        this.s3 = s3;
        this.s3Objects = S3Objects.withPrefix(s3, bucketName, prefix).iterator();

        next();
    }

    public void seek(long timestamp) {
        while (next.timestamp < timestamp && hasNext) {
            next();
        }
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public TripEvent next() {
        String nextLine = null;

        try {
            nextLine = objectStream.readLine();
        } catch (IOException | NullPointerException e ) {}

        if (nextLine == null) {
            if (s3Objects.hasNext()) {
                S3ObjectSummary objectSummary = s3Objects.next();
                String bucket = objectSummary.getBucketName();
                String key = objectSummary.getKey();

                if (s3Object != null) {
                    try {
                        s3Object.close();
                    } catch (IOException e) {
                        LOG.error("failed to close object: {}", e);
                    }
                }

                LOG.info("reading object {}/{}", bucket, key);

                s3Object = s3.getObject(bucket, key);
                InputStream stream = new BufferedInputStream(s3Object.getObjectContent());

                try {
                    stream =  new CompressorStreamFactory().createCompressorInputStream(stream);
                } catch (CompressorException e) {
                    LOG.debug("unable to decompress stream: {}", e.getMessage());
                }

                objectStream = new BufferedReader(new InputStreamReader(stream));

                return next();
            } else {
                hasNext = false;

                return next;
            }
        } else {
            TripEvent result = next;

            try {
                next = new TripEvent(nextLine);

                return result;
            } catch (IllegalArgumentException e) {
                LOG.warn("ignoring line: {}", nextLine);

                return next();
            }
        }
    }
}
