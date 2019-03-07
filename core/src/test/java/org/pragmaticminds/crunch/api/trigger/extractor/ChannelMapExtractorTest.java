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

package org.pragmaticminds.crunch.api.trigger.extractor;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.channel;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.09.2018
 */
public class ChannelMapExtractorTest {
    private Supplier supplier1;
    private Supplier supplier2;
    private Supplier supplier3;

    private MRecord record;

    @Before
    public void setUp() throws Exception {
        supplier1 = channel("test1");
        supplier2 = channel("test2");
        supplier3 = channel("test3");

        Map<String, Object> values = new HashMap<>();
        values.put("test1", 1L);
        values.put("test2", 2L);
        values.put("test3", 3L);

        record = UntypedValues.builder()
                .source("test")
                .timestamp(123L)
                .prefix("")
                .values(values)
                .build();
    }

    @Test
    public void withArrayConstructor() {
        SimpleEvaluationContext context1 = new SimpleEvaluationContext(record);
        SimpleEvaluationContext context2 = new SimpleEvaluationContext(record);

        ChannelMapExtractor extractor = new ChannelMapExtractor(supplier1, supplier2, supplier3);
        ChannelMapExtractor clone = ClonerUtil.clone(extractor);

        executeAndCheckResults("test", context1, extractor);

        executeAndCheckResults("test", context2, clone);
    }

    @Test
    public void withListConstructor() {
        SimpleEvaluationContext context1 = new SimpleEvaluationContext(record);
        SimpleEvaluationContext context2 = new SimpleEvaluationContext(record);

        ChannelMapExtractor extractor = new ChannelMapExtractor(Arrays.asList(supplier1, supplier2, supplier3));
        ChannelMapExtractor clone = ClonerUtil.clone(extractor);

        executeAndCheckResults("test", context1, extractor);
        executeAndCheckResults("test", context2, clone);
    }

    @Test
    public void withMapConstructor() {
        SimpleEvaluationContext context1 = new SimpleEvaluationContext(record);
        SimpleEvaluationContext context2 = new SimpleEvaluationContext(record);

        Map<Supplier, String> map = new HashMap<>();
        map.put(supplier1, "t1");
        map.put(supplier2, "t2");
        map.put(supplier3, "t3");
        ChannelMapExtractor extractor = new ChannelMapExtractor(map);
        ChannelMapExtractor clone = ClonerUtil.clone(extractor);

        executeAndCheckResults("t", context1, extractor);
        executeAndCheckResults("t", context2, clone);
    }

    private void executeAndCheckResults(String prefix, SimpleEvaluationContext context, ChannelMapExtractor extractor) {
        Map<String, Value> result = extractor.extract(context);
        assertEquals(3, result.size());
        assertEquals(1L, (long)result.get(String.format("%s1", prefix)).getAsLong());
        assertEquals(2L, (long)result.get(String.format("%s2", prefix)).getAsLong());
        assertEquals(3L, (long)result.get(String.format("%s3", prefix)).getAsLong());
    }
}