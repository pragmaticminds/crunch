package org.pragmaticminds.crunch.api.trigger.strategy;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;

/**
 * Describes rules to trigger processing of an incoming event.
 * On base of the implemented strategy the result is positive and triggers processing of the EventExtractor
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 26.07.2018
 */
@FunctionalInterface
public interface TriggerStrategy extends Serializable {
    /**
     * Decides if the event is to be triggered by the decision base typed value
     * @param values contains the indicator if processing is to be triggered
     * @return true if triggering was positive
     */
    boolean isToBeTriggered(MRecord values);
}
