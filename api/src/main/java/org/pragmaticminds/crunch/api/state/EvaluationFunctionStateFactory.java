package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Interface for the creation or reuse of EvaluationFunctions in a {@link MultiStepEvaluationFunction}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public interface EvaluationFunctionStateFactory extends Serializable {
    
    /**
     * Creates or resets an instance of a {@link EvaluationFunction} to be used in the
     * {@link MultiStepEvaluationFunction}.
     *
     * @return an instance of {@link EvaluationFunction}
     */
    EvaluationFunction create();
    
    /**
     * Collects all channel identifiers that are used in the inner {@link EvaluationFunction}.
     *
     * @return a {@link List} or {@link Collection} that contains all channel identifiers that are used.
     */
    Collection<String> getChannelIdentifiers();
}
