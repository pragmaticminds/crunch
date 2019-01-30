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
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.09.2018
 */
public class AllChannelMapExtractorTest {
    private AllChannelMapExtractor extractor;
    private AllChannelMapExtractor clone;
    private EvaluationContext context;

    @Before
    public void setUp() throws Exception {
        Map<String, Value> values = new HashMap<>();
        values.put("test1", Value.of(1L));
        values.put("test2", Value.of(2L));
        values.put("test3", Value.of(3L));

        MRecord record = new TypedValues(
                "source",
                123L,
                values
        );
        context = new SimpleEvaluationContext(record);

        extractor = new AllChannelMapExtractor();
        clone = ClonerUtil.clone(extractor);
    }

    @Test
    public void extract() {
        Map<String, Value> results = extractor.extract(context);

        assertEquals(3, results.size());
        assertEquals(1L, (long)results.get("test1").getAsLong());
        assertEquals(2L, (long)results.get("test2").getAsLong());
        assertEquals(3L, (long)results.get("test3").getAsLong());

        results = clone.extract(context);

        assertEquals(3, results.size());
        assertEquals(1L, (long)results.get("test1").getAsLong());
        assertEquals(2L, (long)results.get("test2").getAsLong());
        assertEquals(3L, (long)results.get("test3").getAsLong());
    }
    
    @Test
    public void extractNoValues(){
        MRecord record2 = UntypedValues.builder().source("source").timestamp(123L).values(Collections.emptyMap()).build();
        EvaluationContext<MRecord> context2 = new SimpleEvaluationContext<>(record2);
        Map<String, Value> extract = extractor.extract(context2);
        assertEquals(0, extract.size());
    }
}