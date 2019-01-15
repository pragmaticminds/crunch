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
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Extracts given channels from the {@link EvaluationContext}s inner {@link MRecord}.
 * Can either:
 *  - flat extract channel values and store them by their own name or
 *  - extract channel values and store them by the given mapping name.
 *  Depending on which constructor was called.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.09.2018
 */
class ChannelMapExtractor implements MapExtractor {
    private HashMap<Supplier, String> mappings = null;
    private ArrayList<Supplier> channels = null;

    /**
     * Mapping channel extraction constructor.
     * All passed channels are extracted and keyed by the given name.
     *
     * @param mapping {@link Map} of {@link Supplier} to {@link String}, which is the channel value supplier and the new
     *                           name to be mapped to.
     */
    public ChannelMapExtractor(Map<Supplier, String> mapping) {
        if (mapping == null){
            return;
        }
        this.mappings = new HashMap<>(mapping);
    }

    /**
     * Simple channel extraction constructor.
     * All passed channels are extracted and keyed by their own name.
     *
     * @param channels {@link List} of {@link Supplier}s, that extracts the channel {@link Value}.
     */
    public ChannelMapExtractor(Collection<Supplier> channels){
        if(channels == null){
            return;
        }
        this.channels = new ArrayList<>(channels);
    }

    /**
     * Simple channel extraction constructor.
     * All passed channels are extracted and keyed by their own name.
     *
     * @param channels Array of {@link Supplier}s, that extracts the channel {@link Value}.
     */
    public ChannelMapExtractor(Supplier... channels){
        if(channels == null || channels.length == 0){
            return;
        }
        this.channels = new ArrayList<>(Arrays.asList(channels));
    }

    /**
     * This method extracts a map of {@link Value}s from a {@link EvaluationContext}, in particular from it's
     * {@link MRecord}.
     *
     * @param context the current {@link EvaluationContext} that holds the current {@link MRecord}.
     * @return a {@link Map} of keyed extracted values from the {@link EvaluationContext}s {@link MRecord}.
     */
    @Override
    public Map<String, Value> extract(EvaluationContext context) {
        if(channels != null){
            return channels.stream().collect(Collectors.toMap(
                    Supplier::getIdentifier,
                supplier -> {
                    try {
                        return Value.of(supplier.extract(context.get()));
                    } catch (Exception e) {
                        return null;
                    }
                }
            ));
        }else if(mappings != null){
            return mappings.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getValue,
                entry -> {
                    try {
                        return Value.of(entry.getKey().extract(context.get()));
                    } catch (Exception e) {
                        return null;
                    }
                }
            ));
        }
        return null;
    }
}
