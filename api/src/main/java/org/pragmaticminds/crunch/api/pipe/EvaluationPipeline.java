package org.pragmaticminds.crunch.api.pipe;

import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.exceptions.IdentifierAlreadyExistsException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is a meta class. It describes the structures to build a pipeline for Crunch processing.
 * It holds a list of SubStreams, which are holding lists of {@link EvaluationFunction}s.
 * With this class one can create something like a Flink pipeline.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 01.08.2018
 */
public class EvaluationPipeline<T extends Serializable> implements Serializable {
    private final String identifier;
    private final List<SubStream<T>> subStreams;
    
    /**
     * private constructor for the builder
     * @param identifier of the {@link EvaluationPipeline}
     * @param subStreams of the {@link EvaluationPipeline}, containing all {@link EvaluationFunction}s of the
     *                   pipeline to process
     */
    private EvaluationPipeline(String identifier, List<SubStream<T>> subStreams) {
        this.identifier = identifier;
        this.subStreams = subStreams;
    }
    
    // getter
    public String getIdentifier() {
        return identifier;
    }
    public List<SubStream<T>> getSubStreams() {
        return subStreams;
    }
    
    /**
     * Creates a builder for this class
     * @return a builder
     */
    public static <T extends Serializable> Builder<T> builder() { return new Builder<>(); }
    
    /**
     * this Builder creates new instances of {@link EvaluationPipeline} class
     */
    public static final class Builder<T extends Serializable> implements Serializable {
        private String identifier;
        private List<SubStream<T>> subStreams;
        
        private Builder() {}
    
        /**
         * set the identifier
         * @param identifier
         * @return
         */
        public Builder<T> withIdentifier(String identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder<T> withSubStreams(List<SubStream<T>> subStreams) {
            if(this.subStreams == null){
                this.subStreams = subStreams;
            }else{
                this.subStreams.addAll(subStreams);
            }
            return this;
        }

        public Builder<T> withSubStream(SubStream<T> subStream) {
            if(this.subStreams == null){
                this.subStreams = new ArrayList<>();
            }
            this.subStreams.add(subStream);
            return this;
        }

        public Builder<T> but() {
            return new Builder<T>().withIdentifier(identifier).withSubStreams(subStreams);
        }

        public EvaluationPipeline<T> build() {
            checkConstructorParameters(identifier, subStreams);
            return new EvaluationPipeline<T>(identifier, subStreams);
        }
    
        /**
         * Checks if {@link SubStream} identifiers are null or used multiple times.
         * Checks if the identifier of the {@link EvaluationPipeline} is not null
         * @param identifier of the {@link EvaluationPipeline}
         * @param subStreams list of {@link SubStream}s of the {@link EvaluationPipeline}
         * @throws IdentifierAlreadyExistsException as it says
         */
        private void checkConstructorParameters(String identifier, List<SubStream<T>> subStreams) {
            Preconditions.checkNotNull(identifier, "the identifier of the EvaluationPipeline is not set");
            Preconditions.checkNotNull(subStreams, "the SubStreams of the EvaluationPipeline are not set");
            Preconditions.checkArgument(!subStreams.isEmpty(), "the SubStream of the EvaluationPipeline is empty");
            
            Set<String> identifiers = subStreams.stream()
                .map(SubStream::getIdentifier)
                .peek(identifier1 -> Preconditions.checkNotNull(
                    identifier1,
                    String.format("An identifier is null in EvaluationPipeline: %s", identifier)
                ))
                .collect(Collectors.toSet());
        
            if(identifiers.size() != subStreams.size()){
                throw new IdentifierAlreadyExistsException(
                    String.format("A SubStream identifier is already in use in EvaluationPipeline: %s", identifier)
                );
            }
        }
    }
}
