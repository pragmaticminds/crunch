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

package org.pragmaticminds.crunch.api.state;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.LambdaEvaluationFunction;
import org.pragmaticminds.crunch.api.trigger.comparator.SerializableResultFunction;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class CloneStateEvaluationFunctionFactoryTest {

    private CloneStateEvaluationFunctionFactory factory;

    @Before
    @SuppressWarnings("unchecked") // checked manually
    public void setUp() throws Exception {
        EvaluationFunction function = new LambdaEvaluationFunction(
                context -> {
                },
                (SerializableResultFunction<HashSet<String>>) HashSet::new
        );
        factory = CloneStateEvaluationFunctionFactory.builder()
                .withPrototype(function)
                .build();
    }

    @Test
    public void create() {
        EvaluationFunction evaluationFunction = factory.create();
        assertEquals(new HashSet<>(), evaluationFunction.getChannelIdentifiers());
    }

    @Test
    public void getChannelIdentifiers() {
        Collection<String> channelIdentifiers = factory.getChannelIdentifiers();
        assertEquals(new HashSet<>(), channelIdentifiers);
    }
}