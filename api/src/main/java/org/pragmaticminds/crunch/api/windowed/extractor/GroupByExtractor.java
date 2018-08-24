package org.pragmaticminds.crunch.api.windowed.extractor;

import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.Tuple2;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.windowed.WindowedEvaluationFunction;
import org.pragmaticminds.crunch.api.windowed.extractor.aggregate.Aggregation;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class implements {@link WindowExtractor}. It has aggregators which are holding values of all last
 * values, where the window flag in the {@link WindowedEvaluationFunction} was open. The {@link Aggregation}s accumulate
 * specific values like sums etc. All aggregated values are passed optionally to a {@link GroupAggregationFinalizer},
 * which creates the resulting {@link Event}s. Is the {@link GroupAggregationFinalizer} not set all aggregated values
 * are put into an resulting {@link Event}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class GroupByExtractor implements WindowExtractor {
    private HashMap<String, Tuple2<Aggregation, Supplier>> aggregations;
    private GroupAggregationFinalizer finalizer;
    
    /**
     * Private constructor for the {@link Builder}.
     * @param aggregations a Map of all set {@link Aggregation} implementations like sum, max etc.
     * @param finalizer optional resulting Events creator
     */
    private GroupByExtractor(
        Map<String, Tuple2<Aggregation, Supplier>> aggregations,
        GroupAggregationFinalizer finalizer
    ) {
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
     * Generates resulting {@link Event}s from the applied {@link MRecord}s and calls the contexts collect method on the
     * results.
     *
     * @param context of the current eval call to the parent {@link EvaluationFunction}. also collects the resulting
     *                {@link Event}s
     */
    @Override
    public void finish(EvaluationContext context) {
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
    public static Builder builder() {
        return new Builder();
    }
    
    /** Builder for this class */
    public static final class Builder {
        private Map<String, Tuple2<Aggregation, Supplier>> aggregations;
        // optional
        private GroupAggregationFinalizer finalizer;
        
        private Builder() { /* do nothing */ }
        
        public Builder aggregate(Aggregation aggregation, Supplier supplier){
            aggregate(
                aggregation,
                supplier,
                String.format("%s.%s", aggregation.getIdentifier(), supplier.getIdentifier())
            );
            return this;
        }
        
        public Builder aggregate(Aggregation aggregation, Supplier supplier, String identifier){
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
        private Builder aggregations(Map<String, Tuple2<Aggregation, Supplier>> aggregations) {
            this.aggregations = aggregations;
            return this;
        }
        
        public Builder finalizer(GroupAggregationFinalizer finalizer) {
            this.finalizer = finalizer;
            return this;
        }
    
        public Builder but() {
            return builder()
                .aggregations(aggregations)
                .finalizer(finalizer);
        }
    
        public GroupByExtractor build() {
            if(finalizer == null){
                finalizer = new DefaultGroupAggregationFinalizer();
            }
            return new GroupByExtractor(aggregations, finalizer);
        }
    }
}
