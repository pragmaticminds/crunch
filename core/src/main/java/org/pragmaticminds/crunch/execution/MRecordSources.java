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

import org.pragmaticminds.crunch.api.records.MRecord;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Static Util Method to generate {@link MRecordSource}s from different simple java structures like {@link List}
 * or {@link Iterator}.
 *
 * @author julian
 * Created by julian on 15.08.18
 */
public class MRecordSources {

    /**
     * Never initialize.
     */
    private MRecordSources() {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a {@link MRecordSource} from a list of records
     *
     * @param records List of Records
     * @return MRecordSource
     */
    public static MRecordSource of(MRecord... records) {
        return MRecordSources.of(MRecordSource.Kind.FINITE,
                Arrays.stream(records).iterator());
    }

    /**
     * Creates a {@link MRecordSource} from a list.
     *
     * @param list List of Records
     * @return MRecordSource
     */
    public static MRecordSource of(List<MRecord> list) {
        return MRecordSources.of(MRecordSource.Kind.FINITE,
                list.iterator());
    }

    /**
     * Creates a {@link MRecordSource} from an Iterator.
     *
     * @param iterator Record Iteratorr
     * @return MRecordSource
     */
    public static MRecordSource of(Iterator<MRecord> iterator) {
        return MRecordSources.of(MRecordSource.Kind.UNKNOWN, iterator);
    }

    /**
     * Creates a {@link MRecordSource} from an Iterator with {@link org.pragmaticminds.crunch.execution.MRecordSource.Kind}
     * information.
     *
     * @param kind     Information about the cardinality of the stream
     * @param iterator Record Iteratorr
     * @return MRecordSource
     */
    public static MRecordSource of(MRecordSource.Kind kind, Iterator<MRecord> iterator) {
        return new AbstractMRecordSource(kind) {

            @Override
            public MRecord get() {
                return iterator.next();
            }

            @Override
            public boolean hasRemaining() {
                return iterator.hasNext();
            }
        };
    }
}
