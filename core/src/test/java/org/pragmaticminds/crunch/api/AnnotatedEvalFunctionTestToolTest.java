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

package org.pragmaticminds.crunch.api;

import org.junit.Test;
import org.pragmaticminds.crunch.api.evaluations.annotated.RegexFind2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test for testing class of {@link AnnotatedEvalFunction} classes
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 25.10.2017
 */
public class AnnotatedEvalFunctionTestToolTest {
    private static final Logger logger = LoggerFactory.getLogger(AnnotatedEvalFunctionTestToolTest.class);

    @Test
    public void execute() throws Exception {
        AnnotatedEvalFunctionTestTool testTool = new AnnotatedEvalFunctionTestTool(RegexFind2.class);
        List<Object> parameters = new ArrayList<>();
        parameters.add("test123");

        List<List<Object>> inChannels = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            List<Object> channels = new ArrayList<>();
            channels.add("test1" + i);
            inChannels.add(channels);
        }

        EvalFunctionTestTool.EvaluationTestToolEvents results = testTool.execute(parameters, inChannels);
        results.getOutputs().forEach(output -> logger.debug("output: {}", output));
        results.getEvents().forEach(event -> logger.debug("event: {}", event));

        assertEquals(1, results.getEvents().size());
        assertEquals(100, results.getOutputs().size());
        assertEquals(1, results.getOutputs().stream().filter(Objects::nonNull).collect(Collectors.toList()).size());
    }
}