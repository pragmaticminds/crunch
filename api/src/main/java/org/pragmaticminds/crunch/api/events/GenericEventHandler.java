package org.pragmaticminds.crunch.api.events;

import org.pragmaticminds.crunch.api.EvalFunction;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.GenericEventBuilder;

/**
 * This class handles results of {@link EvalFunction}s. It creates {@link GenericEvent}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.11.2017
 */
@FunctionalInterface // <- so it can be implemented as lambda
public interface GenericEventHandler {
    /**
     * Is called when a new {@link GenericEvent} is fired inside the {@link EvalFunction}
     *
     * @param event that was fired by a {@link EvalFunction}
     */
    void fire(GenericEvent event);

    /**
     * Returns the {@link GenericEventBuilder}
     *
     * @return
     */
    default GenericEventBuilder getBuilder() {
        return GenericEventBuilder.anEvent();
    }
}
