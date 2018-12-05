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

package org.pragmaticminds.crunch.api.windowed.extractor;

import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.Tuple2;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.windowed.WindowedEvaluationFunction;
import org.pragmaticminds.crunch.api.windowed.extractor.aggregate.Aggregation;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class implements {@link WindowExtractor}. It has aggregators which are holding values of all last
 * values, where the window flag in the {@link WindowedEvaluationFunction} was open. The {@link Aggregation}s accumulate
 * specific values like sums etc. All aggregated values are passed optionally to a {@link GroupAggregationFinalizer},
 * which creates the resulting {@link GenericEvent}s. Is the {@link GroupAggregationFinalizer} not set all aggregated values
 * are put into an resulting {@link GenericEvent}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class GroupByExtractor<T extends Serializable> implements WindowExtractor<T> {
    private HashMap<String, Tuple2<Aggregation, Supplier>> aggregations;
    private GroupAggregationFinalizer<T> finalizer;

    /**
     * Private constructor for the {@link Builder}.
     * @param aggregations a Map of all set {@link Aggregation} implementations like sum, max etc.
     * @param finalizer optional resulting Events creator
     */
    private GroupByExtractor(
            Map<String, Tuple2<Aggregation, Supplier>> aggregations,
            GroupAggregationFinalizer<T> finalizer
    ) {
        Preconditions.checkNotNull(finalizer);
        this.aggregations = new HashMap<>(aggregations);
        this.finalizer = finalizer;

        checkValues();
    }

    /** Check if all necessary values are present */
    private void checkValues() {
        Preconditions.checkNotNull(aggregations);
        Preconditions.checkArgument(!aggregations.isEmpty());
    }

    /**
     * This method collects single values for the later made extraction.
     * The value is passed to each {@link Supplier} and the result to the {@link Aggregation}.
     *
     * @param record from the eval call of the {@link WindowedEvaluationFunction}.
     */
    @Override
    @SuppressWarnings("unchecked") // is insured to be safe
    public void apply(MRecord record) {
        aggregations.forEach(
                // aggregate the current record value
                (key, tuple2) -> tuple2.getF0().aggregate(
                        // extract the value of interest from the record with the Supplier
                        (Serializable) tuple2.getF1().extract(record)
                )
        );
    }

    /**
     * Generates resulting {@link GenericEvent}s from the applied {@link MRecord}s and calls the contexts collect method on the
     * results.
     *
     * @param context of the current eval call to the parent {@link EvaluationFunction}. also collects the resulting
     *                {@link GenericEvent}s
     */
    @Override
    public void finish(EvaluationContext<T> context) {
        // extract all the aggregated values from the aggregations and collect them with the key of the aggregations
        Map<String, Object> results = aggregations.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().getF0().getAggregated()
                        )
                );

        // pass the aggregated values to finalizer and return its results
        finalizer.onFinalize(results, context);
    }

    /**
     * Creates a Builder for this class
     * @return a Builder for this class
     */
    public static <T extends Serializable> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder for this class */
    public static final class Builder<T extends Serializable> {
        private Map<String, Tuple2<Aggregation, Supplier>> aggregations;
        // optional
        private GroupAggregationFinalizer<T> finalizer;

        private Builder() { /* do nothing */ }

        public Builder<T> aggregate(Aggregation aggregation, Supplier supplier){
            aggregate(
                    aggregation,
                    supplier,
                    String.format("%s.%s", aggregation.getIdentifier(), supplier.getIdentifier())
            );
            return this;
        }

        public Builder<T> aggregate(Aggregation aggregation, Supplier supplier, String identifier){
            // lazy loading
            if(aggregations == null){
                aggregations = new HashMap<>();
            }

            // create a unique identifier
            String id = identifier;
            int count = 0;
            while(aggregations.containsKey(id)){
                id = identifier + "$" + count;
                count++;
            }

            // aggregate
            aggregations.put(id, new Tuple2<>(aggregation, supplier));
            return this;
        }

        /**
         * only allowed to be called internally, else the naming cannot be guaranteed
         *
         * @param aggregations to be set
         * @return this {@link Builder}
         */
        private Builder<T> aggregations(Map<String, Tuple2<Aggregation, Supplier>> aggregations) {
            this.aggregations = aggregations;
            return this;
        }

        /**
         * The finalizer packs the aggregated values into resulting Ts.
         *
         * If T is GenericEvent the class {@link DefaultGenericEventGroupAggregationFinalizer} can be used.
         *
         * @param finalizer instance of {@link GroupAggregationFinalizer}.
         * @return self
         */
        public Builder<T> finalizer(GroupAggregationFinalizer<T> finalizer) {
            this.finalizer = finalizer;
            return this;
        }

        public Builder<T> but() {
            return new Builder<T>()
                    .aggregations(aggregations)
                    .finalizer(finalizer);
        }

        @SuppressWarnings("unchecked") // must cast
        public GroupByExtractor<T> build() {
            return new GroupByExtractor<>(aggregations, finalizer);
        }
    }
}
