package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;

import java.util.Collection;
import java.util.List;

/**
 * Implementation of the {@link EvaluationFunctionStateFactory}, which creates {@link EvaluationFunction} instances
 * on base of prototype original (as a blueprint) by cloning.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public class CloneStateEvaluationFunctionFactory implements EvaluationFunctionStateFactory {
    private final EvaluationFunction prototype;
    
    /**
     * private constructor for the builder
     * @param prototype of the EvaluationFunction to be cloned and used
     */
    private CloneStateEvaluationFunctionFactory(EvaluationFunction prototype) {
        this.prototype = prototype;
    }
    
    /**
     * Clones the prototype object from instantiation
     *
     * @return a fresh cloned {@link EvaluationFunction} from the prototype
     */
    @Override
    public EvaluationFunction create() {
        return ClonerUtil.clone(prototype);
    }
    
    /**
     * Collects all channel identifiers that are used in the inner {@link EvaluationFunction}.
     *
     * @return a {@link List} or {@link Collection} that contains all channel identifiers that are used.
     */
    @Override
    public Collection<String> getChannelIdentifiers() {
        return prototype.getChannelIdentifiers();
    }
    
    /**
     * creates a builder for this class
     * @return a builder
     */
    public static Builder builder() { return new Builder(); }
    
    /**
     * The builder for this class
     */
    public static final class Builder {
        private EvaluationFunction prototype;
        
        private Builder() { /* do nothing */ }
        
        public Builder withPrototype(EvaluationFunction prototype) {
            this.prototype = prototype;
            return this;
        }
        
        public Builder but() {
            return builder().withPrototype(prototype);
        }
        
        public CloneStateEvaluationFunctionFactory build() {
            return new CloneStateEvaluationFunctionFactory(prototype);
        }
    }
}
