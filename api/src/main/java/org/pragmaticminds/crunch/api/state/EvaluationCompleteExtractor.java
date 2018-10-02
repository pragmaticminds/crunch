package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;
import java.util.Map;

/**
 * This interface if for the extraction of resulting {@link GenericEvent}s after the successful processing of the
 * {@link MultiStepEvaluationFunction} from the resulting Events of all inner {@link EvaluationFunction}s of the
 * {@link MultiStepEvaluationFunction}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
@FunctionalInterface
public interface EvaluationCompleteExtractor<T extends Serializable> extends Serializable {
    
    /**
     * If all states of the {@link MultiStepEvaluationFunction} are done, this method is called.
     * It processes all {@link GenericEvent}s that where passed from inner {@link EvaluationFunction} of the
     * {@link MultiStepEvaluationFunction} and generates outgoing {@link GenericEvent}s, which are than collected by
     * the context
     *
     * @param events incoming values to generate a final resulting T events.
     * @param context has a collect method for the outgoing {@link GenericEvent}s
     */
    void process(Map<String, T> events, EvaluationContext<T> context);
}
