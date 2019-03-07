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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.trigger.comparator.Suppliers;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.HashMap;
import java.util.Map;

/**
 * @author erwin.wagasow
 * created by erwin.wagasow on 24.01.2019
 */
public class BugTicket14MT {
    
    /**
     * A {@link NullPointerException} appeared when not all mapped channels where present in the record.
     */
    @Test
    public void nullPointerExceptionInChannelMapExtractorExtract() {
        
        // create channel mappings for the extractor
        Map<Supplier, String> mapping = new HashMap<>();
        mapping.put(Suppliers.ChannelExtractors.stringChannel("channel1"), "new_channel1");
        mapping.put(Suppliers.ChannelExtractors.stringChannel("channel2"), "new_channel2");
        mapping.put(Suppliers.ChannelExtractors.stringChannel("channel3"), "new_channel3");
        mapping.put(Suppliers.ChannelExtractors.stringChannel("channel4"), "new_channel4");
        mapping.put(Suppliers.ChannelExtractors.stringChannel("channel5"), "new_channel5");
        
        // create the extractor
        ChannelMapExtractor extractor = new ChannelMapExtractor(mapping);
    
        Map<String, Object> values = ImmutableMap.of(
                "channel1", Value.of("test1"),
                "channel2", Value.of("test2"),
                "channel3", Value.of("test3"),
                "channel4", Value.of("test4")
        );
        
        // build a record, that does not hold all necessary channels
        MRecord record = UntypedValues.builder()
                .values(values)
                .build();
        EvaluationContext context = new SimpleEvaluationContext(record);
        Map<String, Value> extractResult = extractor.extract(context);
        
        // now the missing channel names are printed before the NullPointerException occurs
    }
}
