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

import java.time.Instant;
import java.util.*;

/**
 * This is a helper class to generate data for a Sink.
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 21.01.2019
 */
public class ValuesGenerator {
    private static final Random random = new Random();

    /** hidden constructor */
    private ValuesGenerator() {
        /* do nothing */
    }

    public static List<Map<String, Object>> generateDoubleData(int recordCount, int channelCount, String channelNamePrefix){
        ArrayList<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            Map<String, Object> result = new HashMap<>();
            for (int j = 0; j < channelCount; j++) {
                result.put(channelNamePrefix + j, random.nextDouble());
            }
            results.add(result);
        }
        return results;
    }

    public static List<Map<String, Object>> generateMixedData(int recordCount, String channelNamePrefix) {
        ArrayList<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            HashMap<String, Object> result = new HashMap<>();
            result.put(channelNamePrefix + "booleanTrue", true);
            result.put(channelNamePrefix + "booleanFalse", false);
            result.put(channelNamePrefix + "longNegative", -1L*(i+1L));
            result.put(channelNamePrefix + "longZero", 0L);
            result.put(channelNamePrefix + "long", (long) i+1L);
            result.put(channelNamePrefix + "doubleNegative", (double)(i+1)*-1D);
            result.put(channelNamePrefix + "doubleZero", 0D);
            result.put(channelNamePrefix + "double", (double) i+1);
            result.put(channelNamePrefix + "date", Date.from(Instant.now()));
            result.put(channelNamePrefix + "string", "test string");
            result.put(channelNamePrefix + "stringLongNumber", "1000000000");
            results.add(result);
        }
        return results;
    }
}
