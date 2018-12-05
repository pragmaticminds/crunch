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

import akka.Done;
import akka.NotUsed;
import akka.japi.function.Function;
import akka.japi.function.Function2;
import akka.japi.function.Predicate;
import akka.stream.ClosedShape;
import akka.stream.Outlet;
import akka.stream.SinkShape;
import akka.stream.javadsl.*;
import org.pragmaticminds.crunch.api.pipe.*;
import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Factory to create an Akka {@link akka.stream.stage.GraphStage} from a
 * {@link org.pragmaticminds.crunch.api.pipe.EvaluationPipeline}.
 * <p>
 * As input it neets
 * <ul>
 * <li>A source of records</li>
 * <li>An evaluation pipeline</li>
 * <li>An GenericEvent Sink.</li>
 * </ul>
 * <p>
 * This is a pretty technical class.
 *
 * @see for more Information on Akka Streams see https://doc.akka.io/docs/akka/2.5/stream/index.html
 *
 * @author julian
 * Created by julian on 15.08.18
 */
class GraphFactory<T extends Serializable> {

    /**
     * Creates a {@link RunnableGraph} from Akka Streams.
     * This graph can then be Materialized and run.
     *
     * @param source   Source of MRecords
     * @param pipeline Pipeline to use
     * @return RunnableGraph for Materialization.
     */
    RunnableGraph<CompletionStage<Done>> create(
            MRecordSource source,
            EvaluationPipeline<T> pipeline,
            EventSink<T> sink,
            Long watermarkOffsetMs
    ) {
        // Source from the MRecordSourceWrapper
        Source<MRecord, NotUsed> streamSource = Source.fromGraph(new MRecordSourceWrapper(source));
        // The Sink is only for the MRecords, thus ignores them
        Sink<Object, CompletionStage<Done>> streamSink = Sink.ignore();

        // Translate the EvaluationPipeline to Akka Graph
        return RunnableGraph.fromGraph(
                GraphDSL.create(             // we need to reference out's shape in the builder DSL below (in to() function)
                        streamSink,                // previously created sink (Sink)
                        buildGraph(streamSource, pipeline, sink, watermarkOffsetMs)
                ));
    }

    /**
     * Builds the Graph for the {@link EvaluationPipeline} using Akkas {@link GraphDSL}.
     * See https://doc.akka.io/docs/akka/2.5/stream/stream-graphs.html
     *
     * @param streamSource Source
     * @param pipeline     EvaluationPipeline to evaluate
     * @param sink         result output
     * @return Suitable parameter for {@link GraphDSL#create(Function)} method
     */
    @SuppressWarnings("unchecked") // manually checked
    private Function2<GraphDSL.Builder<CompletionStage<Done>>, SinkShape<Object>, ClosedShape> buildGraph(
            Source<MRecord, NotUsed> streamSource,
            EvaluationPipeline<T> pipeline,
            EventSink<T> sink,
            Long watermarkOffsetMs
    ) {
        return (builder, out) -> {  // variables: builder (GraphDSL.Builder) and out (SinkShape)
            final Outlet<MRecord> builderSource = builder.add(streamSource).out();

            GraphDSL.Builder.ForwardOps stream = builder.from(builderSource);
            for (SubStream<T> subStream : pipeline.getSubStreams()) {

                // Generate a Flow from the RecordHandlers
                List<RecordHandler> recordHandlers = subStream.getRecordHandlers();
                // Initialize the record handlers
                recordHandlers.forEach(RecordHandler::init);

                // Generate a Flow from the Evaluation Functions
                List<EvaluationFunction<T>> evalFunctions = subStream.getEvalFunctions();
                // Initialize the eval functions
                evalFunctions.forEach(EvaluationFunction::init);

                // Prepare the stream
                GraphDSL.Builder.ForwardOps outStream = stream
                        .via(builder.add(
                                Flow.of(MRecord.class)
                                        // filter all incoming MRecords with predicate
                                        .filter(record -> subStream.getPredicate().validate(record))
                                        // filter all not relevant MRecords with channels that are never used
                                        .filter(createChannelFilter(subStream))
                                        // merge incoming MRecords
                                        .map(new MergeFunction())
                                )
                        )
                        .via(builder.add(
                                // Sort all incoming records by their timestamp in the time window defined by #watermarkOffsetMs
                                new SortGraphFlow<>(watermarkOffsetMs)
                        ))
                        .via(builder.add(
                                // pass all MRecords to all EvaluationFunctions and RecordHandlers of the current subStream
                                GraphFactory.this.toFlow(recordHandlers, evalFunctions, sink))
                        );

                // Ignore this sink
                outStream.to(out);
            }

            return ClosedShape.getInstance();
        };
    }

    /**
     * Creates a filter that is looking for the availability of the channels that are used.
     *
     * @param subStream that delivers a {@link Collection} of channel identifiers that are used.
     * @return a {@link Predicate} for the type {@link MRecord}.
     */
    private Predicate<MRecord> createChannelFilter(SubStream<T> subStream) {
        ChannelFilter<T> channelFilter = new ChannelFilter<>(subStream);
        return channelFilter::filter;
    }

    /**
     * Helper Method to generate a Flow which does the "Evaluation" and forwards possible Events to the
     * given Sink
     *
     * @param functions Functions to evaluate
     * @param sink      Sink to forward results to
     * @return Flow for usage in {@link #buildGraph(Source, EvaluationPipeline, EventSink, Long)} method
     */
    private Flow<MRecord, MRecord, NotUsed> toFlow(
            List<RecordHandler> recordHandlers,
            List<EvaluationFunction<T>> functions,
            EventSink<T> sink
    ) {
        return Flow
                .of(MRecord.class)
                .map(
                        (Function<MRecord, MRecord>)param -> {
                            for (RecordHandler handler : recordHandlers) {
                                handler.apply(param);
                            }
                            return param;
                        }
                )
                .map(
                        (Function<MRecord, MRecord>) param -> {
                            EvaluationContext<T> context = new EventSinkContext<>(sink);
                            ((EventSinkContext) context).setCurrent(param);
                            for (EvaluationFunction<T> function : functions) {
                                function.eval(context);
                            }
                            return param;
                        }
                );
    }

    /** Wrapps the UntypedValuesMergeFunction as a akka {@link Function} */
    static class MergeFunction extends UntypedValuesMergeFunction implements Function<MRecord, MRecord>{
        @Override
        public MRecord apply(MRecord value) {
            return merge(value);
        }
    }
}
