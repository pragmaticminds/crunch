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

package org.pragmaticminds.crunch.api.windowed.extractor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.api.windowed.extractor.aggregate.Aggregations;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.GenericEventBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.doubleChannel;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class GroupByExtractorTest implements Serializable {

    private GroupByExtractor<GenericEvent> extractor;
    private GroupByExtractor<GenericEvent> extractor2;
    private GroupByExtractor<GenericEvent> clone1;
    private GroupByExtractor<GenericEvent> clone2;
    private ArrayList<MRecord> records;
    private SimpleEvaluationContext<GenericEvent> context1;
    private SimpleEvaluationContext<GenericEvent> context2;
    private String myMin;

    @Before
    public void setUp() {
        // create extractors
        myMin = "my min";
        extractor = GroupByExtractor.<GenericEvent>builder()
                .aggregate(Aggregations.max(), doubleChannel("x"))
                .aggregate(Aggregations.max(), doubleChannel("x"))
                .aggregate(Aggregations.min(), doubleChannel("x"), myMin)
                .finalizer((aggregatedValues, context) ->
                        context.collect(
                                GenericEventBuilder.anEvent()
                                        .withEvent("test")
                                        .withSource("test")
                                        .withTimestamp(System.currentTimeMillis())
                                        .withParameter("max.x", Value.of(aggregatedValues.get("max.x")))
                                        .withParameter("max.x$0", Value.of(aggregatedValues.get("max.x$0")))
                                        .withParameter("min.x", Value.of(aggregatedValues.get(myMin)))
                                        .build()
                        )
                )
                .build();
        clone1 = ClonerUtil.clone(extractor);

        extractor2 = GroupByExtractor.<GenericEvent>builder()
                .aggregate(Aggregations.max(), doubleChannel("x"))
                .aggregate(Aggregations.min(), doubleChannel("x"), myMin)
                .finalizer(new DefaultGenericEventGroupAggregationFinalizer())
                .build();
        clone2 = ClonerUtil.clone(extractor2);

        // create records
        records = new ArrayList<>();
        long timestamp = System.currentTimeMillis();
        for (int i = 1; i < 11; i++) {
            Map<String, Object> valueMap = new HashMap<>();
            valueMap.put("x", (double)i);
            records.add(UntypedValues.builder()
                    .prefix("")
                    .source("test")
                    .timestamp(timestamp)
                    .values(valueMap)
                    .build()
            );
        }
    }

    @Test
    public void extract() {
        context1 = new SimpleEvaluationContext<>(records.get(records.size()-1));

        records.forEach(record -> extractor.apply(record));
        extractor.finish(context1);
        List<GenericEvent> results1 = context1.getEvents();
        Assert.assertNotNull(results1);
        Assert.assertEquals(1, results1.size());
        GenericEvent event1 = results1.get(0);
        Assert.assertNotNull(event1);
        Assert.assertEquals(10D, event1.getParameter("max.x").getAsDouble(), 0.0001);
        Assert.assertEquals(10D, event1.getParameter("max.x$0").getAsDouble(), 0.0001);
        Assert.assertEquals(1D, event1.getParameter("min.x").getAsDouble(), 0.0001);

        // test on clone
        context2 = new SimpleEvaluationContext<>(records.get(records.size()-1));

        records.forEach(record -> clone1.apply(record));
        extractor.finish(context2);
        List<GenericEvent> results2 = context2.getEvents();
        Assert.assertNotNull(results2);
        Assert.assertEquals(1, results2.size());
        GenericEvent event2 = results2.get(0);
        Assert.assertNotNull(event2);
        Assert.assertEquals(10D, event2.getParameter("max.x").getAsDouble(), 0.0001);
        Assert.assertEquals(10D, event2.getParameter("max.x$0").getAsDouble(), 0.0001);
        Assert.assertEquals(1D, event2.getParameter("min.x").getAsDouble(), 0.0001);
    }

    @Test
    public void extractNoFinalizer() {
        context1 = new SimpleEvaluationContext<>(records.get(records.size()-1));

        records.forEach(record -> extractor2.apply(record));
        extractor2.finish(context1);
        List<GenericEvent> results1 = context1.getEvents();
        Assert.assertNotNull(results1);
        Assert.assertEquals(1, results1.size());
        GenericEvent event1 = results1.get(0);
        Assert.assertNotNull(event1);
        Assert.assertEquals(10D, event1.getParameter("max.x").getAsDouble(), 0.0001);
        Assert.assertEquals(1D, event1.getParameter(myMin).getAsDouble(), 0.0001);

        // test on clone2
        context2 = new SimpleEvaluationContext<>(records.get(records.size()-1));

        records.forEach(record -> clone2.apply(record));
        clone2.finish(context2);
        List<GenericEvent> results2 = context2.getEvents();
        Assert.assertNotNull(results2);
        Assert.assertEquals(1, results2.size());
        GenericEvent event2 = results2.get(0);
        Assert.assertNotNull(event2);
        Assert.assertEquals(10D, event2.getParameter("max.x").getAsDouble(), 0.0001);
        Assert.assertEquals(1D, event2.getParameter(myMin).getAsDouble(), 0.0001);
    }

    @Test
    public void builder(){
        Assert.assertNotNull(extractor);
    }
}