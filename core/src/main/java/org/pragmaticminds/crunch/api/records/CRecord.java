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

package org.pragmaticminds.crunch.api.records;

import java.io.Serializable;

/**
 * Generic Crunch Record (thus, CRecord).
 * It forms the base class of all possible Record Streams.
 *
 * Each Record has to have a timestamp.
 *
 * Usually subinterfaces of this interfaces should be used.
 *
 * @author julian
 * Created by julian on 14.08.18
 */
@SuppressWarnings("squid:S1609") // This is NO FunctionalInterface but rather a Marker Interface
public interface CRecord extends Serializable {

    /**
     * Getter
     *
     * @return Timestamp of record as ms since 01.01.1970
     */
    long getTimestamp();

}
