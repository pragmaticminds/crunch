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

package org.pragmaticminds.crunch.api.trigger.comparator;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * This is a abstract implementation of the {@link Supplier}, where the setting of the identifier is handled.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.08.2018
 */
public class NamedSupplier<T extends Serializable> implements Supplier<T> {
    private String identifier;
    private SerializableFunction<MRecord, T> extractLambda;
    private SerializableResultFunction<HashSet<String>> getIdentifiersLambda;

    /**
     * Main constructor with identifier
     *
     * @param identifier identifies this {@link Supplier} implementation
     * @param extractLambda extracts the value of interest from the {@link MRecord}
     */
    @SuppressWarnings("unchecked") // is insured to be safe
    public NamedSupplier(
            String identifier,
            SerializableFunction<MRecord, T> extractLambda,
            SerializableResultFunction<HashSet<String>> getIdentifiersLambda
    ) {
        this.identifier = identifier;
        this.extractLambda = extractLambda;
        this.getIdentifiersLambda = getIdentifiersLambda;
    }

    /**
     * Compares the incoming values with internal criteria and returns a result of T
     *
     * @param values incoming values to be compared to internal criteria
     * @return a result of T
     */
    @Override
    public T extract(MRecord values) {
        return extractLambda.apply(values);
    }

    /**
     * All suppliers have to be identifiable
     *
     * @return String identifier of the {@link Supplier} implementation
     */
    @Override
    public String getIdentifier() {
        return identifier;
    }

    /** @inheritDoc */
    @Override
    public Set<String> getChannelIdentifiers() {
        return getIdentifiersLambda.get();
    }
}
