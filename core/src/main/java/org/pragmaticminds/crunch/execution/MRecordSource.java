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

import java.io.Serializable;

/**
 * Source for {@link MRecordSource} can be finite or infinite.
 *
 * @author julian
 * Created by julian on 15.08.18
 */
public interface MRecordSource extends Serializable {

    /**
     * Request the next record
     *
     * @return record
     */
    MRecord get();

    /**
     * Check wheter more records are available for fetch
     *
     * @return true if records can be fetched using {@link #get()}
     */
    boolean hasRemaining();

    /**
     * Is called before first call to {@link #hasRemaining()} or {@link #get()}.
     */
    void init();

    /**
     * Is called after processing has ended (either by cancelling or by exhausting the source).
     */
    void close();

    /**
     * Returns the Kind of the record source.
     *
     * @return Kind of the source.
     */
    Kind getKind();

    /**
     * Specifies the Kind of this MRecordSource.
     */
    enum Kind {
        FINITE,
        INFINITE,
        UNKNOWN
    }
}
