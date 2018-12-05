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

package org.pragmaticminds.crunch.api.trigger;

import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.trigger.filter.EventFilter;
import org.pragmaticminds.crunch.api.trigger.handler.TriggerHandler;
import org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategy;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.events.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Simplifies the way to implement {@link EvaluationFunction} for typical tasks.
 * The {@link Supplier} pre evaluates the incoming data, so that the {@link TriggerStrategy} can decide
 * if further processing is required in the {@link TriggerHandler}. The {@link TriggerHandler} than generates Event
 * resulting from the incoming data. The optional {@link EventFilter} than filters the resulting {@link Event}s before
 * they are passed to the next processing step.
 *
 *  eval({@link EvaluationContext})
 *        |
 *        |-- {@link MRecord}
 *        |
 *  {@link TriggerStrategy}
 *        |
 *        |-- {@link MRecord}
 *        |
 *  ({@link TriggerHandler})
 *        |
 *        |-- {@link List} of {@link Event}
 *        |
 *  ({@link EventFilter}
 *        |
 *        |-- {@link List} of {@link Event}
 *        |
 *  {@link EvaluationContext}::collect({@link Event})
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.07.2018
 */
public class TriggerEvaluationFunction<T extends Serializable> implements EvaluationFunction<T> {

    private static final Logger logger = LoggerFactory.getLogger(TriggerEvaluationFunction.class);

    private final TriggerStrategy triggerStrategy;
    private final TriggerHandler<T> triggerHandler;
    private final EventFilter<T> filter;

    /**
     * Main constructor of this class
     * @param triggerStrategy decides on the base of the simplified {@link Supplier} result, whether to
     *                        pass the incoming data to the {@link TriggerHandler} or not
     * @param triggerHandler extracts all the necessary values from the {@link MRecord} and creates a resulting Event.
     * @param filter filters the resulting Events after processing if set (not null)
     */
    private TriggerEvaluationFunction(
            TriggerStrategy triggerStrategy,
            TriggerHandler<T> triggerHandler,
            EventFilter<T> filter
    ) {
        Preconditions.checkNotNull(triggerStrategy, "Trigger strategy has to be set");
        Preconditions.checkNotNull(triggerHandler, "Trigger handler has to be set");
        this.triggerStrategy = triggerStrategy;
        this.triggerHandler = triggerHandler;
        this.filter = filter;
    }

    /**
     * Filters the results if filter is set
     *
     * @param record  for filtering purpose
     * @param results for filtering purpose
     * @return the filtered list of results if filter was set, otherwise the original list of results.
     */
    private List<T> filter(MRecord record, List<T> results) {
        if (filter != null) {
            return results.stream()
                    .filter(event -> filter.apply(event, record))
                    .collect(Collectors.toList());
        }
        return results;
    }

    /**
     * Returns all channel identifiers which are necessary for the function to do its job.
     * It is not allowed to return null, an empty set can be returned (but why should??).
     *
     * @return a {@link Set} all channel identifiers that are needed by the Evaluation Function.
     */
    @Override
    public Set<String> getChannelIdentifiers() {
        Set<String> results = new HashSet<>();
        if (filter != null) {
            results.addAll(filter.getChannelIdentifiers());
        }
        results.addAll(triggerStrategy.getChannelIdentifiers());
        return results;
    }

    /**
     * evaluates the incoming {@link TypedValues} from the {@link EvaluationContext} and passes the results
     * back to the collect method of the context
     *
     * @param ctx contains incoming data and a collector for the outgoing data
     */
    @Override
    public void eval(EvaluationContext<T> ctx) {
        MRecord record = ctx.get();

        // check if to be triggered
        if (triggerStrategy.isToBeTriggered(record)) {

            // log if debug is active
            if (logger.isDebugEnabled()) {
                logger.debug("Trigger Strategy Triggered for record {}", record);
            }

            // create a simple context for the triggerHandler to extract resulting Events
            SimpleEvaluationContext<T> simpleContext = new SimpleEvaluationContext<>(ctx.get());

            // extract resulting Events
            triggerHandler.handle(simpleContext);

            // get the result Events from the simple context
            List<T> results = simpleContext.getEvents();

            // collect results
            if(results != null && !results.isEmpty()) {
                // filter results if filter set
                results = filter(record, results);
                results.forEach(ctx::collect);
            }
        }
    }


    /**
     * Creates a new instance of the {@link Builder} for this class.
     *
     * @return a {@link Builder} for this class.
     */
    public static <T extends Serializable> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Builder of this class.
     */
    public static class Builder<T extends Serializable> {
        private TriggerStrategy triggerStrategy;
        private TriggerHandler<T> triggerHandler;
        private EventFilter<T> filter;

        public Builder<T> withTriggerStrategy(TriggerStrategy triggerStrategy){
            this.triggerStrategy = triggerStrategy;
            return this;
        }

        public Builder<T> withTriggerHandler(TriggerHandler<T> triggerHandler){
            this.triggerHandler = triggerHandler;
            return this;
        }

        public Builder<T> withFilter(EventFilter<T> filter){
            this.filter = filter;
            return this;
        }

        public TriggerEvaluationFunction<T> build() {
            return new TriggerEvaluationFunction<>(triggerStrategy, triggerHandler, filter);
        }
    }
}
