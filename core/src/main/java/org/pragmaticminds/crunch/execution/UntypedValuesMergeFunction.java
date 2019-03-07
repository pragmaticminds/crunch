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
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.security.InvalidParameterException;

/**
 * This class implements the interface {@link MergeFunction}.
 * It takes {@link UntypedValues} or {@link UntypedValues} as incoming values and produces outgoing {@link UntypedValues}.
 * It combines the values of the internal {@link UntypedValues} with the incoming {@link MRecord} values and returns
 * the result in the {@link #merge(MRecord)} method.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 02.10.2018
 */
public class UntypedValuesMergeFunction implements MergeFunction<MRecord, MRecord> {

    private UntypedValues values = null;

    /**
     * Merges incomming values to a aggregated value containing all other sub values from before.
     *
     * @param currentValue to be merged
     * @return merged value
     */
    @Override
    public MRecord merge(MRecord currentValue) {
        if (!(currentValue instanceof UntypedValues)) {
            throw new InvalidParameterException(String.format(
                    "ValuesMergeFunction currently only supports UntypedValues and not %s",
                    currentValue.getClass().getName()
            ));
        }

        // Do the mapping
        values = mapWithoutState(values, currentValue);
        return values;
    }

    /**
     * Internal method that does the merging of the state.
     * Does not fetch / rewrite the Function's state.
     *
     * @param currentValues the inner values that is the merge from all calls before {@link MRecord}s.
     * @param newValues the value from the current call tho the {@link #merge(MRecord)} method.
     * @return the merged {@link UntypedValues} object from the inner #currentValues and the incoming {@link UntypedValues}
     */
    UntypedValues mapWithoutState(UntypedValues currentValues, MRecord newValues) {
        // Init the valueState object on first value object
        UntypedValues state;
        if (currentValues == null) {
            state = (UntypedValues) newValues;
        } else {
            state = currentValues.merge((UntypedValues) newValues);
        }
        return state;
    }
}
