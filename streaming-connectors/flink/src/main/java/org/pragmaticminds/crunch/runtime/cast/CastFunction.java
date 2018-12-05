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

package org.pragmaticminds.crunch.runtime.cast;

import org.apache.flink.api.common.functions.MapFunction;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;

/**
 * @author kerstin
 * Created by kerstin on 03.11.17.
 * This function casts UntypedValues to TypedValues.
 */
public class CastFunction implements MapFunction<UntypedValues, TypedValues> {

    /**
     * Casts an UntypedValues object to TypedValues object which is then returned
     *
     * @param untypedValues to be casted
     * @return the casted TypedValues
     * @throws Exception when a cast is not successful
     */
    @Override
    public TypedValues map(UntypedValues untypedValues) {
        return untypedValues.toTypedValues();
    }
}
