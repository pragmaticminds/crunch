package org.pragmaticminds.crunch.api.pipe;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.ValueEvent;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.runtime.cast.CastFunction;
import org.pragmaticminds.crunch.runtime.merge.ValuesMergeFunction;
import org.pragmaticminds.crunch.runtime.sort.SortFunction;
import org.pragmaticminds.crunch.runtime.sort.ValueEventAssigner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class creates on base of the {@link EvaluationPipeline} construct a Flink pipeline.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 01.08.2018
 */
public class CrunchFlinkPipelineFactory implements Serializable {
    
    /**
     * This is the factories create method. It creates internally a Flink pipeline and returns its output {@link DataStream}
     * of {@link Event}s. It implements {@link DataStream} processing routes in a {@link java.util.stream.Stream} manner.
     *
     * @param in the in going {@link DataStream} of {@link UntypedValues} as Source for processing
     * @param pipeline this describes the pipelines in a {@link EvaluationPipeline} way
     * @return outgoing {@link DataStream} of {@link Event}s as output of the created Flink pipeline
     */
    public DataStream<Event> create(DataStream<UntypedValues> in, EvaluationPipeline pipeline) {
        List<DataStream<Event>> results = new ArrayList<>();
        for (SubStream subStream:  pipeline.getSubStreams()) {
            results.add(in
                // filter with SubStream.Predicate
                .filter(createFilter(subStream.getPredicate()::validate))
                
                .map(untypedValues -> (ValueEvent) untypedValues)
                
                // set sort window
                .assignTimestampsAndWatermarks(new ValueEventAssigner(subStream.getSortWindowMs()))
    
                .map(valueEvent -> (UntypedValues) valueEvent)
    
                // cast from UntypedValues to TypedValues
                .map(new CastFunction())
                
                .map(untypedValues -> (ValueEvent) untypedValues)
                
                .keyBy(x -> 0)
                
                // sort the messages
                .process(new SortFunction())
                
                .map(valueEvent -> (TypedValues) valueEvent)
                
                // must be handled with keying
                .keyBy(x -> 0)
                
                // merge old values with new values
                .map(new ValuesMergeFunction())
                
                // set key to every value by source name
                .keyBy(x -> 0)
                
                // process the data one after an other in all EvaluationFunctions
                .process(EvaluationProcessFunction.builder().withEvaluationFunctions(subStream.getEvalFunctions()).build())
                
                // convert to DataStream<Event>
                .forward());
        }
        
        // combine the resulting DataStreams of all created SubStreams and return it
        return results.stream()
            // combine all results DataStreams
            .reduce(DataStream::union)
            // get or default
            .orElse(null);
    }
    
    /**
     * This function creates a filter of a {@link SubStreamPredicate}
     *
     * @param predicate to be put in a filter
     * @return a {@link FilterFunction} which contains the predicate
     */
    private FilterFunction<UntypedValues> createFilter(SubStreamPredicate predicate) {
        return predicate::validate;
    }
}
