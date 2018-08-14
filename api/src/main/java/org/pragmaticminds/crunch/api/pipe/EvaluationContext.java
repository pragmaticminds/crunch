package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.pipe.timer.Timer;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;

import java.io.Serializable;

/**
 * The context for evaluation in a {@link EvaluationFunction} containing ways to process incoming and
 * outgoing data.
 *
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 03.08.2018
 */
@SuppressWarnings("squid:S1610") // not converting into an interface
public abstract class EvaluationContext implements Serializable {
    /**
     * delivers the next {@link TypedValues} data to be processed
     * @return the next record to be processed
     */
    public abstract TypedValues get();

    /**
     * collects the resulting {@link Event}s of processing
     * @param event result of the processing of an {@link EvaluationFunction}
     */
    public abstract void collect(Event event);
    
    /**
     * Getter for an {@link EventBuilder}
     * @return the Builder for {@link Event}s
     */
    public EventBuilder getEventBuilder(){
        return EventBuilder.anEvent();
    }
    
    /**
     * Creates a Timer that is boxed in {@link Timer} class object
     * @param evaluationFunction
     * @return a Timer boxed in {@link Timer} class object
     */
    public abstract Timer createNewTimer(EvaluationFunction evaluationFunction);
}
