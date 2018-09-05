package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;
import java.util.Map;

/**
 * This interface if for the extraction of resulting {@link Event}s after the successful processing of the
 * {@link MultiStepEvaluationFunction} from the resulting Events of all inner {@link EvaluationFunction}s of the
 * {@link MultiStepEvaluationFunction}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
@FunctionalInterface
public interface EvaluationCompleteExtractor extends Serializable {
    
    /**
     * If all states of the {@link MultiStepEvaluationFunction} are done, this method is called.
     * It processes all {@link Event}s that where passed from inner {@link EvaluationFunction} of the
     * {@link MultiStepEvaluationFunction} and generates outgoing {@link Event}s, which are than collected by
     * the context
     *
     * @param events incoming values to generate a final resulting {@link Event}
     * @param context has a collect method for the outgoing {@link Event}s
     */
    void process(Map<String, Event> events, EvaluationContext context);
}
