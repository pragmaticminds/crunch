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

package org.pragmaticminds.crunch.api2;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.execution.UntypedValuesMergeFunction;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Example visitor that is used to transfer a crunch stream into an Enumerable.
 *
 * @param <T>     Type of the Source events
 * @param <EVENT> Type of the generated Events
 */
public class Linq4jImplementor<T extends MRecord, EVENT extends Serializable> {

    private final Enumerable<T> input;
    private final Predicate<T> predicate;
    private final Function<T, Object> groupAssigner;
    private final List<EvaluationANdHandler<EVENT>> evaluations;

    public Linq4jImplementor(Root<T> root) {
        final ImplementingVisitor implementingVisitor = new ImplementingVisitor();
        root.accept(implementingVisitor);
        this.input = implementingVisitor.input;
        this.predicate = implementingVisitor.predicate;
        this.groupAssigner = implementingVisitor.groupAssigner;
        this.evaluations = implementingVisitor.eh;
    }

    public Enumerator<Void> implement() {
        Enumerable<T> datastream;
        if (predicate != null) {
            datastream = input.where(predicate::test);
        } else {
            datastream = input;
        }
        // Start the evaluation
        return implementEvaluation(datastream, groupAssigner, evaluations);
    }

    private <EVENT extends Serializable> Enumerator<Void> implementEvaluation(Enumerable<T> in, Function<T, Object> groupAssigner, List<EvaluationANdHandler<EVENT>> evaluations) {
        // Need state for each group and each Evaluation...
        final Map<GroupEvaluation<EVENT>, EvaluationFunction<EVENT>> states = new HashMap<>();
        final Map<Object, UntypedValuesMergeFunction> mergedState = new HashMap<>();
        final Enumerator<T> enumerator = in.enumerator();

        return new Enumerator<Void>() {

            long mergeTimeNs = 0;

            @Override public Void current() {
                return null;
            }

            @Override public boolean moveNext() {
                // Evaluate until an event is found
                while (enumerator.moveNext()) {
                    // Create merged state
                    // Get hash
                    final Object hash = groupAssigner.apply(enumerator.current());
                    // Merge with state for previous values for this group
                    mergedState.computeIfAbsent(hash, k -> new UntypedValuesMergeFunction());
                    final MRecord mergedRecord;
                    try {
                        final long start = System.nanoTime();
                        mergedRecord = mergedState.get(hash).merge(enumerator.current());
                        final long stop = System.nanoTime();
                        mergeTimeNs += (stop - start);
                    } catch (IllegalArgumentException e) {
                        // Catch "older values" and simply skip
                        // TODO fix this
                        continue;
                    }
                    // For each evaluation
                    for (EvaluationANdHandler<EVENT> eh : evaluations) {
                        final SimpleEvaluationContext<EVENT> ctx = new SimpleEvaluationContext<>(mergedRecord);
                        final GroupEvaluation<EVENT> group = new GroupEvaluation<>(hash, eh.evaluation);
                        states.computeIfAbsent(group, g -> ClonerUtil.clone(g.evaluation));
                        states.get(group).eval(ctx);

                        for (EVENT event : ctx.getEvents()) {
                            eh.handler.accept(event);
                        }
                    }
                    return true;
                }
                return false;
            }

            @Override public void reset() {

            }

            @Override public void close() {

            }
        };
    }

    public static class EvaluationANdHandler<EVENT extends Serializable> {

        private final EvaluationFunction<EVENT> evaluation;
        private final Consumer<EVENT> handler;

        public EvaluationANdHandler(EvaluationFunction<EVENT> evaluation, Consumer<EVENT> handler) {
            this.evaluation = evaluation;
            this.handler = handler;
        }

        public EvaluationFunction<EVENT> getEvaluation() {
            return evaluation;
        }

        public Consumer<EVENT> getHandler() {
            return handler;
        }
    }

    public static class GroupEvaluation<T extends Serializable> {

        private final Object hash;
        private final EvaluationFunction<T> evaluation;

        public GroupEvaluation(Object hash, EvaluationFunction<T> evaluation) {
            this.hash = hash;
            this.evaluation = evaluation;
        }

        public Object getHash() {
            return hash;
        }

        public EvaluationFunction<T> getEvaluation() {
            return evaluation;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupEvaluation<?> that = (GroupEvaluation<?>) o;
            return Objects.equals(hash, that.hash) &&
                    Objects.equals(evaluation, that.evaluation);
        }

        @Override public int hashCode() {
            return Objects.hash(hash, evaluation);
        }
    }

    private static class ImplementingVisitor<IN, KEY, EVENT extends Serializable> implements StreamNodeVisitor<Void> {

        private Enumerable<IN> input;
        private Predicate<IN> predicate;
        private Function<KEY, Object> groupAssigner;
        private List<EvaluationANdHandler<EVENT>> eh = new ArrayList<>();

        @Override public <IN1> Void visit(Root<IN1> root) {
            input = ((Enumerable<IN>) root.getValues());
            root.getChildren().forEach(c -> c.accept(this));
            return null;
        }

        @Override public <IN1> Void visit(Filter<IN1> filter) {
            predicate = (Predicate<IN>) filter.predicate;
            filter.getChildren().forEach(c -> c.accept(this));
            return null;
        }

        @Override public <KEY1, T> Void visit(GroupBy<KEY1, T> groupBy) {
            groupAssigner = (Function<KEY, Object>) groupBy.groupAssigner;
            groupBy.getChildren().forEach(c -> c.accept(this));
            return null;
        }

        @Override public <IN1, EVENT1 extends Serializable> Void visit(Evaluate<IN1, EVENT1> evaluate) {
            final EvaluationFunction<EVENT> evaluation = (EvaluationFunction<EVENT>) evaluate.getEvaluation();
            for (StreamNode<EVENT1, ?> child : evaluate.getChildren()) {
                assert child instanceof ResultHandler;
                this.eh.add(new EvaluationANdHandler<>(evaluation, ((ResultHandler<EVENT>) child).getConsumer()));
            }
            return null;
        }

        @Override
        public <EVENT extends Serializable> Void visit(ResultHandler<EVENT> handler) {
            throw new IllegalStateException("This should not happen!");
        }

    }
}
