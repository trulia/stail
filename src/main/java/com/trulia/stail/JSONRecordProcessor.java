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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class JSONRecordProcessor implements IRecordProcessor {
    private static final Logger logger = LoggerFactory.getLogger(JSONRecordProcessor.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void initialize(String shardId) {
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        records.forEach(record -> {
            try {
                byte[] bytes = new byte[record.getData().remaining()];
                record.getData().get(bytes);
                System.out.println(mapper.writeValueAsString(mapper.readTree(bytes)));
            } catch (IOException e) {
                logger.error("", e);
            }
        });
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
    }
}
