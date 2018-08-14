package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;
import java.util.Map;

/**
 * This extractor class processes error situations that happen while processing in the {@link ChainedEvaluationFunction}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
@FunctionalInterface
public interface StateErrorExtractor extends Serializable {
    
    /**
     * If any states of the {@link ChainedEvaluationFunction} produce an Exception or a timeout exception is raised,
     * this method is called.
     * This processes the so far generated result events and the occurred exception to generate final outgoing
     * {@link Event}s
     *
     * @param events incoming values to generate a final resulting {@link Event}
     * @param ex {@link Exception} of the error situation
     * @param context has a collect method for the outgoing {@link Event}s
     */
    void process(Map<String, Event> events, Exception ex, EvaluationContext context);
}
