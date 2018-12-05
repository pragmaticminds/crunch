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

import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.channel;

/**
 * Only test the factory methods.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.09.2018
 */
public class ExtractorsTest {

    @Test
    public void allChannelMapExtractor() {
        assertNotNull(Extractors.allChannelMapExtractor());
    }

    @Test
    public void channelMapExtractor() {
        assertNotNull(Extractors.channelMapExtractor(channel("test1")));
    }

    @Test
    public void channelMapExtractor1() {
        assertNotNull(Extractors.channelMapExtractor(Arrays.asList(channel("test1"))));
    }

    @Test
    public void channelMapExtractor2() {
        Map<Supplier, String> map = new HashMap<>();
        map.put(channel("test1"), "t1");
        assertNotNull(Extractors.channelMapExtractor(map));

        // serializable test
        assertNotNull(ClonerUtil.clone(Extractors.channelMapExtractor(map)));
    }
}