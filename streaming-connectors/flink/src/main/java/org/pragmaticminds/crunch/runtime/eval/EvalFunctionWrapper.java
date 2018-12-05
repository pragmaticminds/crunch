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

package org.pragmaticminds.crunch.runtime.eval;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.pragmaticminds.crunch.api.EvalFunction;
import org.pragmaticminds.crunch.api.EvalFunctionCall;
import org.pragmaticminds.crunch.api.events.GenericEventHandler;
import org.pragmaticminds.crunch.api.records.DataType;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Generates a Wrapper Node around a CRUNCH {@link EvalFunction}
 * or in more Detail an {@link EvalFunctionCall}.
 * <p>
 * For each new Value the EvalFunction is evaluated and if a Event is given, it is returned downstream.
 * <p>
 * Uses a {@link java.util.concurrent.BlockingQueue} internally to "flush" all the Events that are received through
 * the {@link GenericEventHandler} interfaces fire method on the next call of the processElement call.
 *
 * @author julian
 * Created by julian on 05.11.17
 */
public class EvalFunctionWrapper extends ProcessFunction<MRecord, GenericEvent>
        implements GenericEventHandler, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(EvalFunctionWrapper.class);

    private final HashMap<String, DataType> channelsAndTypes;
    private EvalFunctionCall call;
    private LinkedBlockingQueue<GenericEvent> eventBuffer;

    // Flink State
    private transient ValueState<EvalFunction> valueState;

    public EvalFunctionWrapper(EvalFunctionCall call) {
        this.call = call;
        channelsAndTypes = new HashMap<>(call.getChannelsAndTypes());
        this.eventBuffer = new LinkedBlockingQueue<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Init State
        // Get Value State
        ValueStateDescriptor<EvalFunction> descriptor = new ValueStateDescriptor<>(
                // state name
                "evalFunction-state",
                // type information of state
                TypeInformation.of(EvalFunction.class));
        valueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(MRecord value, Context ctx, Collector<GenericEvent> out) throws Exception {
        // Do the eval

        // Assemble the right "parameter array"
        Map<String, Value> map = createTypedChannelMap(value);
        if (!map.isEmpty()) {
            Map<String, Object> renamedMap = call.getChannelNamesMapping().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            entry -> map.get(entry.getValue())));

            if (valueState.value() == null) {
                // init
                EvalFunction evalFunction = this.call.getEvalFunction();
                evalFunction.setup(call.getLiterals(), this);
                valueState.update(evalFunction);
            }

            EvalFunction evalFunction = valueState.value();


            //update the eval function with the new event handler
            evalFunction.setEventHandler(this);


            try {
                evalFunction.eval(value.getTimestamp(), renamedMap);
            } catch (Exception e) {
                logger.warn("Problem during Evaluation of evalfunction " + evalFunction + " with map " + renamedMap, e);
            }
            valueState.update(evalFunction);

            // Return elements if some are in the buffer
            while (!eventBuffer.isEmpty()) {
                GenericEvent event = eventBuffer.poll();
                // Set the source of the current value as the events source. This is allowed as the stream is always keyed on this.
                event.setEventSource(value.getSource());
                out.collect(event);
            }
        }
    }

    /**
     * Creates a Map that contains only the channels that are requested by the eval function.
     * Futhermore all values are casted to the requested Types.
     *
     * @param value TypedValues to use
     * @return Casted map
     */
    Map<String, Value> createTypedChannelMap(MRecord value) {
        // Check if all keys are in the values
        List<String> notFoundKeys = channelsAndTypes.entrySet().stream()
                .filter(entry -> !value.getChannels().contains(entry.getKey()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (!notFoundKeys.isEmpty()) {
            logger.trace("No key matching SPS parameter found for evaluation {}, the missing channels are {}",
                    call.getEvalFunction().getFunctionDef().getSignature().getName(),
                    notFoundKeys);
            return Collections.emptyMap();
        }

        return channelsAndTypes.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> value.getValue(entry.getKey())));
    }

    @Override
    public void fire(GenericEvent event) {
        eventBuffer.add(event);
    }
}
