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

package org.pragmaticminds.crunch.api.trigger.strategy;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.values.TypedValues;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;

/**
 * This is a helper class for the {@link TriggerStrategies} class and other implementations.
 * It prevents duplicate code. It memorizes the last values of the decision base.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 13.08.2018
 */
public abstract class MemoryTriggerStrategy<T extends Serializable> implements TriggerStrategy {

    protected ArrayList<T> lastDecisionBases = new ArrayList<>();
  protected Supplier<T> supplier;
    protected int          bufferSize;
    protected T            initialValue;

    /**
     * Main constructor
     * @param supplier of the decision base, which extracts relevant values from the {@link TypedValues}
     * @param bufferSize defines how many steps are to be safed
     */
    public MemoryTriggerStrategy(Supplier<T> supplier, int bufferSize, T initialValue) {
        this.supplier = supplier;
        this.bufferSize = bufferSize;
        this.initialValue = initialValue;
        if(initialValue != null){
            lastDecisionBases.add(initialValue);
        }
    }

    /**
     * This method decides whether it is to be triggered or not.
     * This method is implemented in here an leads out the part of decision making into the method
     * isToBeTriggered(Boolean decisionBase).
     * @param values for the {@link Supplier} to extract the relevant data
     * @return true if triggered, otherwise false
     */
    @Override
    public boolean isToBeTriggered(MRecord values) {
        T decisionBase = supplier.extract(values);

        // ignore null values
        if(decisionBase == null){
            return false;
        }

        boolean result = isToBeTriggered(decisionBase);
        lastDecisionBases.add(decisionBase);
        if(lastDecisionBases.size() > bufferSize){
            lastDecisionBases.remove(0);
        }
        return result;
    }

    /**
     * This method is to be implemented by the user of this class, with the final decision making.
     * @param decisionBase the extracted value
     * @return true if triggered, otherwise false
     */
    public abstract boolean isToBeTriggered(T decisionBase);

    /**
     * Returns all channel identifiers which are necessary for the function to do its job.
     * It is not allowed to return null, an empty set can be returned (but why should??).
     *
     * @return a {@link Set} all channel identifiers that are needed by the Evaluation Function.
     */
    @Override
    public Set<String> getChannelIdentifiers() {
        return supplier.getChannelIdentifiers();
    }
}
