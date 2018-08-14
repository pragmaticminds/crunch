package org.pragmaticminds.crunch.api.events;

import org.pragmaticminds.crunch.api.EvalFunction;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;

/**
 * This class handles results of {@link EvalFunction}s. It creates {@link Event}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.11.2017
 */
@FunctionalInterface // <- so it can be implemented as lambda
public interface EventHandler {
    /**
     * Is called when a new {@link Event} is fired inside the {@link EvalFunction}
     *
     * @param event that was fired by a {@link EvalFunction}
     */
    void fire(Event event);

    /**
     * Returns the {@link EventBuilder}
     *
     * @return
     */
    default EventBuilder getBuilder() {
        return EventBuilder.anEvent();
    }
}
