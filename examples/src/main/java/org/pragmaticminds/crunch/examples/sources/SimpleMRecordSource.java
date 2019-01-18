/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.pragmaticminds.crunch.examples.sources;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.execution.AbstractMRecordSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This is a simple implementation of a finite MRecordSource.
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 18.01.2019
 */
public class SimpleMRecordSource extends AbstractMRecordSource {
    private static final Logger logger = LoggerFactory.getLogger(SimpleMRecordSource.class);
    private final Long intervalMs;
    private final transient List<Map<String, Object>> values;

    private SimpleMRecordSource(List<Map<String, Object>> values, Long intervalMs) {
        super(Kind.FINITE);
        this.values = values;
        this.intervalMs = intervalMs;
    }

    public static SimpleMRecordSource fromData(List<Map<String, Object>> values, long intervalMs){
        return new SimpleMRecordSource(values, intervalMs);
    }

    @Override
    public MRecord get() {
        if(values.isEmpty()){
            return null;
        }
        MRecord record = new UntypedValues("simpleSource", Instant.now().getEpochSecond(), "", values.get(0));
        try {
            TimeUnit.MILLISECONDS.sleep(intervalMs);
        } catch (InterruptedException e) {
            logger.warn("could not sleep!", e);
            Thread.currentThread().interrupt();
        }
        values.remove(0);
        return record;
    }

    @Override
    public boolean hasRemaining() {
        return !values.isEmpty();
    }
}
