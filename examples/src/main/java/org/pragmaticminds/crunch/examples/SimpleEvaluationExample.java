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

package org.pragmaticminds.crunch.examples;

import org.pragmaticminds.crunch.api.pipe.*;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.GenericEventBuilder;
import org.pragmaticminds.crunch.examples.sinks.SimpleGenericEventSink;
import org.pragmaticminds.crunch.examples.sources.ValuesGenerator;
import org.pragmaticminds.crunch.execution.CrunchExecutor;
import org.pragmaticminds.crunch.execution.EventSink;
import org.pragmaticminds.crunch.execution.MRecordSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.pragmaticminds.crunch.examples.sources.SimpleMRecordSource.fromData;

/**
 * This Class is a example for a simple evaluation crunch processing.
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 18.01.2019
 */
public class SimpleEvaluationExample implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SimpleEvaluationExample.class);

    private ArrayList<Map<String, Object>> inData;
    private MRecordSource source;
    private EvaluationPipeline<GenericEvent> pipeline;
    private List<SubStream<GenericEvent>> subStreams;
    private EventSink<GenericEvent> sink;
    private transient CrunchExecutor executor;

    public SimpleEvaluationExample() {
        source = createSource();
        pipeline = generatePipeline();
        sink = generateSink();

        executor = new CrunchExecutor(source, pipeline, sink);
    }

    public List<Map<String, Object>> getInData() {
        if(inData == null){
            inData = new ArrayList<>(ValuesGenerator.generateDoubleData(100, 20, "channel"));
        }
        return inData;
    }
    public MRecordSource getSource() {
        return source;
    }
    public EvaluationPipeline<GenericEvent> getPipeline() {
        return pipeline;
    }
    public List<SubStream<GenericEvent>> getSubStreams() {
        return subStreams;
    }
    public EventSink<GenericEvent> getSink() {
        return sink;
    }
    public CrunchExecutor getExecutor() {
        return executor;
    }

    public static void main(String[] args) {
        logger.info("application started");
        SimpleEvaluationExample simpleEvaluationExample = new SimpleEvaluationExample();
        simpleEvaluationExample.executor.run();
    }

    protected EventSink<GenericEvent> generateSink() {
        return new SimpleGenericEventSink();
    }

    protected EvaluationPipeline<GenericEvent> generatePipeline() {
        Map<String, Object> firstRecordData = getInData().get(0);
        Set<String> channelNames = firstRecordData.keySet();
        subStreams = generateSubStreams(channelNames);
        return EvaluationPipeline.<GenericEvent>builder().withIdentifier("testPipeline").withSubStreams(subStreams).build();
    }

    protected MRecordSource createSource() {
        return fromData(getInData(), 1000);
    }

    protected List<SubStream<GenericEvent>> generateSubStreams(Set<String> channels) {
        EvaluationFunction<GenericEvent> evaluationFunction = generateEvaluationFunction(channels);

        List<SubStream<GenericEvent>> resultingSubStreams = new ArrayList<>();
        resultingSubStreams.add(SubStream.<GenericEvent>builder()
                .withEvaluationFunction(evaluationFunction)
                .withIdentifier("subStream")
                .withPredicate(values -> true)
                .build());
        return resultingSubStreams;
    }

    protected EvaluationFunction<GenericEvent> generateEvaluationFunction(Set<String> channels) {
        HashSet<String> channels1 = new HashSet<>(channels);
        return new LambdaEvaluationFunction<>(
                // evaluation lambda expression. This evaluation function takes the incoming record and creates an
                // GenericEvent out of it.
                evaluationContext -> evaluationContext.collect(
                        GenericEventBuilder
                                .anEvent()
                                .withEvent("resultEvent")
                                // collect all values from the record and put them into the result event.
                                .withParameters(
                                        evaluationContext
                                                .get()
                                                .getChannels()
                                                .stream()
                                                .collect(
                                                        Collectors.toMap(
                                                                channel -> channel,
                                                                channel -> evaluationContext.get().getValue(channel)
                                                        )
                                                )
                                )
                                // take the source from the record into the resulting event
                                .withSource(evaluationContext.get().getSource())
                                // take the timestamp from the record into the resulting event
                                .withTimestamp(evaluationContext.get().getTimestamp())
                                .build()
                ),
                // relevant channels getter lambda expression. Evaluation functions get only records if relevant
                // channels are present. That for each EvaluationFunction must know it's channels.
                () -> channels1
        );
    }

}
