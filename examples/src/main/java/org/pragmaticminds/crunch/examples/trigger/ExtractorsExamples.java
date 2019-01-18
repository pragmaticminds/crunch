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

package org.pragmaticminds.crunch.examples.trigger;

import org.pragmaticminds.crunch.api.trigger.comparator.Suppliers;
import org.pragmaticminds.crunch.api.trigger.extractor.Extractors;
import org.pragmaticminds.crunch.api.trigger.handler.GenericExtractorTriggerHandler;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hold different examples for the usage of Extractors based on the {@link TriggerEvaluationFunctionExample}.
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 21.01.2019
 */
public class ExtractorsExamples {

    private static String resultEventName = "resultEvent";

    /** hidden constructor */
    private ExtractorsExamples(){ /* do nothing */ }

    /** This example shows an Extractor, that extracts all channels from the incoming record. */
    public static class AllChannelsExtractorExample extends TriggerEvaluationFunctionExample {
        public static void main(String[] args) {
            AllChannelsExtractorExample example = new AllChannelsExtractorExample();
            example.getExecutor().run();
        }

        @Override
        public GenericExtractorTriggerHandler generateTriggerHandler(Set<String> channels) {
            return new GenericExtractorTriggerHandler(
                    resultEventName,
                    Extractors.allChannelMapExtractor(new HashSet<>(channels))
            );
        }
    }

    /** This example shows an Extractor, that is extracting different channels and is keeping the naming. */
    public static class MapExtractorsCollection extends TriggerEvaluationFunctionExample {
        public static void main(String[] args) {
            AllChannelsExtractorExample example = new AllChannelsExtractorExample();
            example.getExecutor().run();
        }

        @Override
        public GenericExtractorTriggerHandler generateTriggerHandler(Set<String> channels) {
            return new GenericExtractorTriggerHandler(
                    resultEventName,
                    Extractors.channelMapExtractor(
                            channels
                                    .stream()
                                    .map(Suppliers.ChannelExtractors::doubleChannel)
                                    .collect(Collectors.toList())
                    )
            );
        }
    }

    /** This example shows an Extractor, that is extracting different channels and modifies the naming of the channels. */
    public static class MappedExtractor extends TriggerEvaluationFunctionExample {
        public static void main(String[] args) {
            AllChannelsExtractorExample example = new AllChannelsExtractorExample();
            example.getExecutor().run();
        }

        @Override
        public GenericExtractorTriggerHandler generateTriggerHandler(Set<String> channels) {
            return new GenericExtractorTriggerHandler(
                    resultEventName,
                    Extractors.channelMapExtractor(
                            channels
                                    .stream()
                                    .map(Suppliers.ChannelExtractors::doubleChannel)
                                    .collect(Collectors.toMap(
                                            supplier -> supplier,
                                            supplier -> "extra_prefix_" + supplier.getIdentifier()
                                    ))
                    )
            );
        }
    }
}
