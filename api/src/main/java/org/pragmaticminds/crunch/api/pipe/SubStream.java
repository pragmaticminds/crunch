package org.pragmaticminds.crunch.api.pipe;

import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a meta class. It describes the stream of a {@link EvaluationPipeline}.
 * It contains {@link EvaluationFunction}s to describe what is to be processed.
 * Classes that are using this meta class for creation of pipelines of any kind are usually do the flowing steps:
 *   1. use the predicate to filter only relevant values.
 *   2. cast the incoming {@link UntypedValues} to {@link TypedValues}
 *   3. sort the incoming messages in a time window
 *   4. process the messages through the evaluationFunctions
 *   5. collect the resulting Events to pass them to a sink
 *
 * With the "predicate" incoming values are filtered, to getValue only the relevant values for processing in the
 * evaluationFunctions. On creation of this class all evaluationFunctions are cloned, so the originals can be used
 * more often, without caring for cross access to their members from different pipelines.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 01.08.2018
 */
public class SubStream implements Serializable {
    
    private final String identifier;
    private final SubStreamPredicate predicate;
    private final List<EvaluationFunction> evaluationFunctions;
    private final List<RecordHandler> recordHandlers;
    private final long sortWindowMs;
    
    /**
     * private constructor for the builder
     * @param identifier the name of the {@link SubStream}
     * @param predicate the filter criteria for incoming {@link UntypedValues}
     * @param evaluationFunctions a list of all {@link EvaluationFunction}s to be processed in
     *                            that {@link SubStream}
     * @param sortWindowMs describes a time window in which incoming {@link UntypedValues} can be sorted before they
     *                   are processed
     */
    private SubStream(
        String identifier, SubStreamPredicate predicate, List<EvaluationFunction> evaluationFunctions, List<RecordHandler> recordHandlers, long sortWindowMs
    ) {
        this.identifier = identifier;
        this.predicate = predicate;
        this.evaluationFunctions = new ArrayList<>();
        if(evaluationFunctions != null && !evaluationFunctions.isEmpty()){
            for (EvaluationFunction evaluationFunction : evaluationFunctions) {
                this.evaluationFunctions.add(ClonerUtil.clone(evaluationFunction));
            }
        }
        this.recordHandlers = new ArrayList<>();
        if(recordHandlers != null && !recordHandlers.isEmpty()){
            for(RecordHandler recordHandler : recordHandlers){
                this.recordHandlers.add(ClonerUtil.clone(recordHandler));
            }
        }
        this.sortWindowMs = sortWindowMs;
    }
    
    // getter
    public String getIdentifier() {
        return identifier;
    }
    
    public SubStreamPredicate getPredicate() {
        return predicate;
    }
    
    /**
     * This getter returns always the same instances, that are in use and have a state!!!
     * So be sure to clone them before reusing in an other place and that you are not affected of their states.
     * @return the instances of the {@link EvaluationFunction}s of this class that are in use and have a state!!!
     */
    public List<EvaluationFunction> getEvalFunctions() {
        return evaluationFunctions;
    }
    
    /**
     * This getter returns always the same instances, that are in use and have a state!!!
     * So be sure to clone them before reusing in an other place and that you are not affected of their states.
     * @return the instances of the {@link RecordHandler}s of this class that are in use and have a state!!!
     */
    public List<RecordHandler> getRecordHandlers(){
        return recordHandlers;
    }
    
    public long getSortWindowMs() {
        return sortWindowMs;
    }
    
    public static Builder builder() { return new Builder(); }
    
    /**
     * Creates new instances of the {@link SubStream} class.
     * Also Checks if all necessary values are set.
     */
    public static final class Builder implements Serializable {
        private String                   identifier;
        private SubStreamPredicate       predicate;
        private List<EvaluationFunction> evaluationFunctions;
        private long                     sortWindow;
        private List<RecordHandler>      recordHandlers;
        
        private Builder() {}
        
        public Builder withIdentifier(String identifier) {
            this.identifier = identifier;
            return this;
        }
        
        public Builder withPredicate(SubStreamPredicate predicate) {
            this.predicate = predicate;
            return this;
        }
        
        public Builder withEvaluationFunctions(List<EvaluationFunction> evaluationFunctions) {
            if(this.evaluationFunctions == null){
                this.evaluationFunctions = evaluationFunctions;
            }else{
                this.evaluationFunctions.addAll(evaluationFunctions);
            }
            return this;
        }
        
        public Builder withEvaluationFunction(EvaluationFunction evaluationFunction) {
            if(this.evaluationFunctions == null){
                this.evaluationFunctions = new ArrayList<>();
            }
            this.evaluationFunctions.add(evaluationFunction);
            return this;
        }
        
        public Builder withRecordHandlers(List<RecordHandler> recordHandlers){
            if(this.recordHandlers == null){
                this.recordHandlers = recordHandlers;
            }else{
                this.recordHandlers.addAll(recordHandlers);
            }
            return this;
        }
        
        public Builder withRecordHandler(RecordHandler recordHandler){
            if(this.recordHandlers == null){
                this.recordHandlers = new ArrayList<>();
            }
            this.recordHandlers.add(recordHandler);
            return this;
        }
        
        public Builder withSortWindow(long sortWindow) {
            this.sortWindow = sortWindow;
            return this;
        }
        
        public Builder but() {
            return builder().withIdentifier(identifier)
                .withPredicate(predicate)
                .withEvaluationFunctions(evaluationFunctions)
                .withSortWindow(sortWindow);
        }
        
        public SubStream build() {
            checkParameters();
            return new SubStream(identifier, predicate, evaluationFunctions, recordHandlers, sortWindow);
        }
        
        /**
         * Checks all given values to the Builder for consistency
         */
        private void checkParameters() {
            Preconditions.checkNotNull(identifier, "identifier is not set for SubStream");
            Preconditions.checkNotNull(predicate, "predicate is not set for SubStream");
            Preconditions.checkArgument(
                (evaluationFunctions == null || !evaluationFunctions.isEmpty())
                && (recordHandlers == null || !recordHandlers.isEmpty()),
                "evaluationFunctions list and recordHandlers list is empty in SubStream"
            );
            if(evaluationFunctions != null){
                evaluationFunctions.forEach(Preconditions::checkNotNull);
            }
            if(recordHandlers != null){
                recordHandlers.forEach(Preconditions::checkNotNull);
            }
        }
    }
}
