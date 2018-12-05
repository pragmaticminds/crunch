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

package org.pragmaticminds.crunch.chronicle;

import org.junit.Test;
import org.mockito.Mockito;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author julian
 * Created by julian on 21.08.18
 */
public class ChronicleSourceTest {

    @Test
    public void create_noInitialize() {
        ChronicleSource.ChronicleConsumerFactory factory = Mockito.mock(ChronicleSource.ChronicleConsumerFactory.class);
        ChronicleSource source = new ChronicleSource(factory);

        source.close();

        // Assert no instantiation
        verify(factory, never()).create();
    }

    @Test
    public void initialize() {
        ChronicleConsumer consumer = Mockito.mock(ChronicleConsumer.class);
        ChronicleSource source = new ChronicleSource(() -> consumer);

        source.init();

        assertTrue(source.hasRemaining());

        source.get();
        source.close();

        // Assert poll on source
        verify(consumer, times(1)).poll();
    }
}