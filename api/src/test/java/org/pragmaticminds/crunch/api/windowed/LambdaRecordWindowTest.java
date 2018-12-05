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

package org.pragmaticminds.crunch.api.windowed;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.records.MRecord;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class LambdaRecordWindowTest {
    private LambdaRecordWindow window;
    private LambdaRecordWindow clone;

    @Before
    public void setUp() throws Exception {
        window = new LambdaRecordWindow(
                record -> true,
                () -> new ArrayList<>(Collections.singletonList("test"))
        );
        clone = ClonerUtil.clone(window);
    }

    @Test
    public void inWindow() {
        assertTrue(window.inWindow(mock(MRecord.class)));
        assertTrue(clone.inWindow(mock(MRecord.class)));
    }

    @Test
    public void getChannelIdentifiers() {
        assertTrue(window.getChannelIdentifiers().contains("test"));
        assertTrue(clone.getChannelIdentifiers().contains("test"));
    }
}