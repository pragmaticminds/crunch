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

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Extracts all channel values into a {@link Map} from the {@link MRecord} inside the {@link EvaluationContext}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.09.2018
 */
class AllChannelMapExtractor implements MapExtractor {

    /**
     * Extracts all channels from the {@link MRecord}.
     * This method extracts a map of {@link Value}s from a {@link EvaluationContext}, in particular from it's
     * {@link MRecord}.
     *
     * @param context the current {@link EvaluationContext} that holds the current {@link MRecord}.
     * @return a {@link Map} of keyed extracted values from the {@link EvaluationContext}s {@link MRecord}.
     */
    @Override
    public Map<String, Value> extract(EvaluationContext context) {
        return context.get().getChannels().stream().collect(Collectors.toMap(
                channel -> channel,
                channel -> context.get().getValue(channel)
        ));
    }
}
