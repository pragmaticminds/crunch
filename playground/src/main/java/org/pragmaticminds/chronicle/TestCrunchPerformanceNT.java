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

package org.pragmaticminds.chronicle;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.ValueIn;
import org.pragmaticminds.crunch.api.pipe.EvaluationPipeline;
import org.pragmaticminds.crunch.api.pipe.SubStream;
import org.pragmaticminds.crunch.api.trigger.TriggerEvaluationFunction;
import org.pragmaticminds.crunch.api.trigger.comparator.Suppliers;
import org.pragmaticminds.crunch.api.trigger.extractor.Extractors;
import org.pragmaticminds.crunch.api.trigger.handler.GenericExtractorTriggerHandler;
import org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategies;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.execution.CrunchExecutor;
import org.pragmaticminds.crunch.execution.EventSink;
import org.pragmaticminds.crunch.source.FileMRecordSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author kerstin
 * Created by kerstin on 16.01.18
 */
public class TestCrunchPerformanceNT {

    public static final int NUMBER_OF_RECORDS = 10000;

    static ArrayList<SubStream<GenericEvent>> getSubStreams() {
        List<String> machines = Arrays.asList("LHL_01", "LHL_02", "LHL_03", "LHL_04", "LHL_05", "LHL_06");
        List<String> mixers = Arrays.asList("Mixer01", "Mixer02");


        ArrayList<SubStream<GenericEvent>> subStreams = new ArrayList<>();

        for (String machineName : machines) {
            subStreams.add(
                    SubStream.<GenericEvent>builder()
                            // set the name of the SubStream
                            .withIdentifier(machineName)

                            // set the sort window queue
                            .withSortWindow(100L)

                            // only pass MRecords where the source is the same as machineName
                            .withPredicate(x -> x.getSource().equals(machineName))

                            // NEW_COMPLETE_CYCLE function
                            .withEvaluationFunction(TriggerEvaluationFunction.<GenericEvent>builder()
                    // set trigger
                    .withTriggerStrategy(
                            // set on value change
                            TriggerStrategies.onChange(
                                    // set channel value extractor
                                    Suppliers.ChannelExtractors.stringChannel(
                                            // set channel name
                                            "DB2521_LOGIN_Stellage"
                                    )
                            )
                    )
                    // set event creator
                    .withTriggerHandler(new GenericExtractorTriggerHandler(
                            // set event name
                            "RACK_ARRIVED_AT_MACHINE",
                            // mapping of channel names to event parameter names
                            Extractors.channelMapExtractor(
                                    Collections.singletonMap(
                                            // supplier for channel value
                                            Suppliers.ChannelExtractors.channel(
                                                    // map from record channel
                                                    "DB2521_LOGIN_Stellage"
                                            ),
                                            // to event parameter name
                                            "rfid"
                                    )
                            )
                    ))
                    .build())


                            .build());
        }

//        for (String mixerName : mixers) {
//            subStreams.add(
//                    SubStream.<GenericEvent>builder()
//                            // set sub stream name
//                            .withIdentifier(mixerName)
//
//                            // set the sort window queue
//                            .withSortWindow(100)
//
//                            // only pass MRecords where the source starts with "mixer"
//                            .withPredicate(x -> x.getSource().equals(mixerName))
//
//                            // set the evaluation functions
//                            .withEvaluationFunctions(IntStream.rangeClosed(1, 6)
//                                    .mapToObj(
//                                            MixerMixtureDetectionFactory::create
//                                    )
//                                    // collect the created TriggerEvaluationFunctions
//                                    .collect(Collectors.toList()) // return in the top
//                            )
//                            .build());
//        }
        return subStreams;
    }

    public static void main(String... ignored) {

        EvaluationPipeline<GenericEvent> eventEvaluationPipeline = EvaluationPipeline.<GenericEvent>builder().withIdentifier("CRUNCH_TEST").withSubStreams(getSubStreams()).build();


        FileMRecordSource kafkaMRecordSource = new FileMRecordSource("src/test/resources/sample.txt");
        List<GenericEvent> events = new ArrayList<>();

        CrunchExecutor crunchExecutor = new CrunchExecutor(kafkaMRecordSource, eventEvaluationPipeline,
                new EventSink<GenericEvent>() {

                    @Override
                    public void handle(GenericEvent genericEvent) {
                        events.add(genericEvent);
                    }
                }
        );
        crunchExecutor.run();
    }

}
