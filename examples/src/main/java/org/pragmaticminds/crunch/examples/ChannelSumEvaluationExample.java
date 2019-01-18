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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.pragmaticminds.crunch.examples.sources.SimpleMRecordSource.fromData;

/**
 * This Class is a example for a simple evaluation crunch processing, where a sum over all channel values is created and
 * written into the resulting {@link GenericEvent}.
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 18.01.2019
 */
public class ChannelSumEvaluationExample extends SimpleEvaluationExample {

    private static final Logger logger = LoggerFactory.getLogger(ChannelSumEvaluationExample.class);

    public ChannelSumEvaluationExample() {
        super();
    }

    public static void main(String[] args) {
        logger.info("application started");
        ChannelSumEvaluationExample channelSumEvaluationExample = new ChannelSumEvaluationExample();
        channelSumEvaluationExample.getExecutor().run();
    }

    @Override
    protected EvaluationFunction<GenericEvent> generateEvaluationFunction(Set<String> channels) {
        HashSet<String> channels1 = new HashSet<>(channels);
        return new LambdaEvaluationFunction<>(
                // evaluation lambda expression. This evaluation function takes the incoming record and creates an
                // GenericEvent out of it.
                evaluationContext -> evaluationContext.collect(
                        GenericEventBuilder
                                .anEvent()
                                .withEvent("resultEvent")
                                // collect all values from the record, make a sum and put it into the result event.
                                .withParameter("SumOverAllChannels", evaluationContext.get().getChannels().stream().mapToDouble(channel -> evaluationContext.get().getDouble(channel)).sum())
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
