package org.pragmaticminds.crunch.execution;

import akka.Done;
import akka.NotUsed;
import akka.japi.function.Function;
import akka.japi.function.Function2;
import akka.stream.ClosedShape;
import akka.stream.Outlet;
import akka.stream.SinkShape;
import akka.stream.javadsl.*;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.EvaluationPipeline;
import org.pragmaticminds.crunch.api.pipe.SubStream;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.security.InvalidParameterException;
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
 * <li>An Event Sink.</li>
 * </ul>
 * <p>
 * This is a pretty technical class.
 *
 * @see for more Information on Akka Streams see https://doc.akka.io/docs/akka/2.5/stream/index.html
 *
 * @author julian
 * Created by julian on 15.08.18
 */
public class GraphFactory {

    /**
     * Creates a {@link RunnableGraph} from Akka Streams.
     * This graph can then be Materialized and run.
     *
     * @param source   Source of MRecords
     * @param pipeline Pipeline to use
     * @return RunnableGraph for Materialization.
     */
    public RunnableGraph<CompletionStage<Done>> create(MRecordSource source, EvaluationPipeline pipeline, EventSink sink) {
        // Source from the MRecordSourceWrapper
        Source<MRecord, NotUsed> streamSource = Source.fromGraph(new MRecordSourceWrapper(source));
        // The Sink is only for the MRecords, thus ignores them
        Sink<Object, CompletionStage<Done>> streamSink = Sink.ignore();

        // Translate the EvaluationPipeline to Akka Graph
        return RunnableGraph.fromGraph(
                GraphDSL.create(             // we need to reference out's shape in the builder DSL below (in to() function)
                        streamSink,                // previously created sink (Sink)
                        buildGraph(streamSource, pipeline, sink)
                ));
    }

    /**
     * Builds the Graph for the {@link EvaluationPipeline} using Akkas {@link GraphDSL}.
     * See https://doc.akka.io/docs/akka/2.5/stream/stream-graphs.html
     *
     * @param streamSource Source
     * @param pipeline     EvaluationPipeline to evaluate
     * @param sink
     * @return Suitable parameter for {@link GraphDSL#create(Function)} method
     */
    private Function2<GraphDSL.Builder<CompletionStage<Done>>, SinkShape<Object>, ClosedShape> buildGraph(Source<MRecord, NotUsed> streamSource, EvaluationPipeline pipeline, EventSink sink) {
        return new Function2<GraphDSL.Builder<CompletionStage<Done>>, SinkShape<Object>, ClosedShape>() {
            @Override
            public ClosedShape apply(GraphDSL.Builder<CompletionStage<Done>> builder, SinkShape<Object> out) throws Exception {  // variables: builder (GraphDSL.Builder) and out (SinkShape)
                final Outlet<MRecord> builderSource = builder.add(streamSource).out();

                GraphDSL.Builder.ForwardOps stream = builder.from(builderSource);
                for (SubStream subStream : pipeline.getSubStreams()) {
                    // Generate a Flow from the Evaluation Functions
                    List<EvaluationFunction> evalFunctions = subStream.getEvalFunctions();
                    // Initialize the eval function
                    evalFunctions.forEach(EvaluationFunction::init);
                    // Prepare the sream
                    stream = stream
                            .via(builder.add(
                                    Flow.of(MRecord.class)
                                            // Filter substream
                                            .filter(record -> subStream.getPredicate().validate(record))
                                            .map(new MergeFunction())
                                    )
                            )
                            .via(builder.add(GraphFactory.this.toFlow(evalFunctions, sink)));
                }
                // Ignore this sink
                stream.to(out);

                return ClosedShape.getInstance();
            }
        };
    }

    /**
     * Helper Method to generate a Flow which does the "Evaluation" and forwards possible Events to the
     * given Sink
     *
     * @param functions Functions to evaluate
     * @param sink      Sink to forward results to
     * @return Flow for usage in {@link #buildGraph(Source, EvaluationPipeline, EventSink)} method
     */
    private Flow<MRecord, MRecord, NotUsed> toFlow(List<EvaluationFunction> functions, EventSink sink) {
        return Flow.of(MRecord.class).map(new Function<MRecord, MRecord>() {
            @Override
            public MRecord apply(MRecord param) throws Exception {
                EvaluationContext context = new EventSinkContext(sink);
                ((EventSinkContext) context).setCurrent(param);
                for (EvaluationFunction function : functions) {
                    function.eval(context);
                }
                return param;
            }
        });
    }

    /**
     * Todo this is a cheap copy of ValuesMergeFunction and has to be generified!
     */
    class MergeFunction implements Function<MRecord, MRecord> {

        private TypedValues values = null;

        @Override
        public MRecord apply(MRecord value) throws Exception {
            // Merge the Records
            TypedValues currentValue;
            if (TypedValues.class.isInstance(value)) {
                currentValue = (TypedValues) value;
            } else if (UntypedValues.class.isInstance(value)) {
                currentValue = ((UntypedValues) value).toTypedValues();
            } else {
                throw new InvalidParameterException("ValuesMergeFunction currently only supports TypedValues and " +
                        "UntypedValues and not " + value.getClass().getName());
            }
            // Do the mapping
            values = mapWithoutState(values, currentValue);
            return values;
        }

        /**
         * Internal method that does the merging of the state.
         * Does not fetch / rewrite the Function's state.
         *
         * @param currentValues
         * @param newValues
         * @return
         */
        TypedValues mapWithoutState(TypedValues currentValues, TypedValues newValues) {
            // Init the valueState object on first value object
            TypedValues state;
            if (currentValues == null) {
                state = newValues;
            } else {
                state = currentValues.merge(newValues);
            }
            return state;
        }

    }
}
