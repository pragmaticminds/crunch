package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;

import java.io.Serializable;

/**
 * Interface for the creation or reuse of EvaluationFunctions in a {@link ChainedEvaluationFunction}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
@FunctionalInterface
public interface EvaluationFunctionStateFactory extends Serializable {
    
    /**
     * Creates or resets an instance of a {@link EvaluationFunction} to be used in the {@link ChainedEvaluationFunction}
     * @return an instance of {@link EvaluationFunction}
     */
    EvaluationFunction create();
}
