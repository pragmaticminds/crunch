package org.pragmaticminds.crunch.api.pipe;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.runtime.merge.ValuesMergeFunction;
import org.pragmaticminds.crunch.runtime.sort.SortFunction;
import org.pragmaticminds.crunch.runtime.sort.ValueEventAssigner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class creates on base of the {@link EvaluationPipeline} construct a Flink pipeline.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 01.08.2018
 */
public class CrunchFlinkPipelineFactory<T extends Serializable> implements Serializable {
    
    /**
     * This is the factories create method. It creates internally a Flink pipeline and returns its output {@link DataStream}
     * of {@link GenericEvent}s. It implements {@link DataStream} processing routes in a {@link java.util.stream.Stream} manner.
     *
     * @param in the in going {@link DataStream} of {@link UntypedValues} as Source for processing
     * @param pipeline this describes the pipelines in a {@link EvaluationPipeline} way
     * @param clazz type of T
     * @return outgoing {@link DataStream} of T as output of the created Flink pipeline
     */
    @SuppressWarnings("unchecked") // is manually checked
    public DataStream<T> create(DataStream<MRecord> in, EvaluationPipeline<T> pipeline, Class<T> clazz) {
        List<DataStream<T>> results = new ArrayList<>();
        for (SubStream<T> subStream:  pipeline.getSubStreams()) {
            SingleOutputStreamOperator<MRecord> sortedIn = in
                
                // filter with SubStream.Predicate
                .filter(createFilter(subStream.getPredicate()))
    
                .keyBy(x -> 0)
    
                // filter all messages where the channels used are not inside
                .filter(createChannelFilter(subStream))
    
                // set sort window
                .assignTimestampsAndWatermarks(new ValueEventAssigner(subStream.getSortWindowMs()))
                
                // must be handled with keying
                .keyBy(x -> 0)
                
                // sort the messages
                .process(new SortFunction());
            
            // branch out the stream for the RecordHandlers
            if (subStream.getRecordHandlers() != null && !subStream.getRecordHandlers().isEmpty()) {
                sortedIn
                        .process(
                                RecordProcessFunction.builder()
                                        .withRecordHandlers(
                                                subStream.getRecordHandlers()
                                        )
                                        .build()
                        )
                        .name("handler called")
                        // pseudo sink -> makes sure all values are processed
                        .addSink(new SinkFunction<Void>() {
                            @Override
                            public void invoke(Void value, Context context) throws Exception {
                                /* no op */
                            }
                        });
            }
            
            // branch out the stream for the EvaluationFunctions
            DataStream<T> dataStream = sortedIn
                //// must be handled with keying
                .keyBy(x -> 0)

                // merge old values with new values
                .map(new ValuesMergeFunction())
        
                // set key to every value by source name
                .keyBy(x -> 0)
        
                // process the data one after an other in all EvaluationFunctions
                .process(
                    EvaluationProcessFunction.<T>builder()
                        .withEvaluationFunctions(subStream.getEvalFunctions())
                        .build()
                )
                .returns(clazz)
        
                // convert to DataStream<GenericEvent>
                .forward();
            
            results.add(dataStream);
        }
        
        // combine the resulting DataStreams of all created SubStreams and return it
        return results.stream()
            // combine all results DataStreams
            .reduce(DataStream::union)
            // getValue or default
            .orElse(null);
    }
    
    /**
     * Creates a filter that is looking for the availability of the channels that are used.
     *
     * @param subStream that delivers a {@link Collection} of channel identifiers that are used.
     * @return a {@link FilterFunction} for the type {@link MRecord}.
     */
    private FilterFunction<MRecord> createChannelFilter(SubStream subStream) {
        return record -> {
            Collection<String> recordChannels = record.getChannels();
            Collection<String> subStreamChannels = subStream.getChannelIdentifiers();
            for (String recordChannel : recordChannels){
                if(subStreamChannels.contains(recordChannel)){
                    return true;
                }
            }
            return false;
        };
    }
    
    /**
     * This function creates a filter of a {@link SubStreamPredicate}
     *
     * @param predicate to be put in a filter
     * @return a {@link FilterFunction} which contains the predicate
     */
    private FilterFunction<MRecord> createFilter(SubStreamPredicate predicate) {
        return predicate::validate;
    }
}
