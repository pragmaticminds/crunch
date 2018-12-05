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

import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author julian
 * Created by julian on 15.08.18
 */
public class MRecordSourcesTest {

    @Test
    public void of_Array() {
        MRecordSource source = MRecordSources.of(new UntypedValues(), new UntypedValues());

        assertTwoElements(source);
    }

    @Test
    public void of_List() {
        MRecordSource source = MRecordSources.of(Arrays.asList(new UntypedValues(), new UntypedValues()));

        assertTwoElements(source);
    }

    @Test
    public void of_Iterator() {
        Iterator<MRecord> iterator = Arrays.<MRecord>asList(new UntypedValues(), new UntypedValues()).iterator();
        MRecordSource source = MRecordSources.of(iterator);

        assertTwoElements(source);
        assertEquals(MRecordSource.Kind.UNKNOWN, source.getKind());
    }

    @Test
    public void of_IteratorWithKind() {
        Iterator<MRecord> iterator = Arrays.<MRecord>asList(new UntypedValues(), new UntypedValues()).iterator();
        MRecordSource source = MRecordSources.of(MRecordSource.Kind.FINITE, iterator);

        assertTwoElements(source);
        assertEquals(MRecordSource.Kind.FINITE, source.getKind());
    }

    /**
     * Asserts that the source has exactly two elements.
     *
     * @param source
     */
    private static void assertTwoElements(MRecordSource source) {
        assertTrue(source.hasRemaining());
        source.get();
        assertTrue(source.hasRemaining());
        source.get();
        assertFalse(source.hasRemaining());
    }
}