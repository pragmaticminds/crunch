package org.pragmaticminds.crunch.api.trigger.handler;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.trigger.TriggerEvaluationFunction;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;

/**
 * This class handles the situation when a {@link TriggerEvaluationFunction} is triggered.
 * It creates a proper result to the triggering.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.09.2018
 */
@FunctionalInterface
public interface TriggerHandler<T extends Serializable> extends Serializable {
    
    /**
     * When a {@link TriggerEvaluationFunction} is triggered, it calls this method to generate a proper result.
     *
     * @param context of the current processing. It holds the current MRecord and it takes the resulting {@link Event}
     *                objects.
     */
    void handle(EvaluationContext<T> context);
}
