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

package org.pragmaticminds.crunch.api.pipe;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class LambdaEvaluationFunctionTest {

    private final HashSet arrayListMock = mock(HashSet.class);
    private LambdaEvaluationFunction function;
    private boolean evalCalled = false;

    @Before
    @SuppressWarnings("unchecked") // manually checked
    public void setUp() throws Exception {
        function = new LambdaEvaluationFunction(
                context -> evalCalled = true,
                () -> arrayListMock
        );
    }

    @Test
    public void eval() {
        function.eval(mock(EvaluationContext.class));
        assertTrue(evalCalled);
    }

    @Test
    public void getChannelIdentifiers() {
        Collection<String> channelIdentifiers = function.getChannelIdentifiers();
        assertEquals(arrayListMock, channelIdentifiers);
    }
}