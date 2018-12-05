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

package org.pragmaticminds.crunch.execution;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.10.2018
 */
public class TimestampSortFunctionTest implements Serializable {

    private TimestampSortFunction<String> sortFunction;
    private TimestampSortFunction<String> clone;

    @Before
    public void setUp() throws Exception {
        sortFunction = new TimestampSortFunction<String>(50);
        clone = ClonerUtil.clone(sortFunction);
    }

    @Test
    public void process() {
        sortFunction.process(1L, 2L, "test1");
        clone.process(1L, 2L, "test1");
    }

    @Test
    public void onTimer() {
        sortFunction.onTimer(2L);
        clone.onTimer(2L);
    }

    @Test
    public void sorting() {
        TimestampSortFunction<String> function = new TimestampSortFunction<>();
        TimestampSortFunction<String> clone = ClonerUtil.clone(function);
        innerSorting(function);
        innerSorting(clone);
    }

    private void innerSorting(TimestampSortFunction<String> function) {
        function.process(60L, 10L, "test5");
        function.process(0L, 10L, "test6"); // should be discarded
        function.process(25L, 10L, "test3");
        function.process(55L, 10L, "test4");
        function.process(15L, 10L, "test1");
        function.process(20L, 10L, "test2");

        // should ignore "test4" and "test5"
        Collection<String> strings = function.onTimer(50L);

        assertEquals(3, strings.size());
        assertEquals(Arrays.asList("test1", "test2", "test3"), strings);
    }
}