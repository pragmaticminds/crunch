package org.pragmaticminds.crunch.api.trigger.strategy;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;
import java.util.Set;

/**
 * Describes rules to trigger processing of an incoming event.
 * On base of the implemented strategy the result is positive and triggers processing of the EventExtractor
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 26.07.2018
 */
public interface TriggerStrategy extends Serializable {
    /**
     * Decides if the event is to be triggered by the decision base typed value
     * @param values contains the indicator if processing is to be triggered
     * @return true if triggering was positive
     */
    boolean isToBeTriggered(MRecord values);

    /**
     * Returns all channel identifiers which are necessary for the function to do its job.
     * It is not allowed to return null, an empty set can be returned (but why should??).
     *
     * @return a {@link Set} all channel identifiers that are needed by the Evaluation Function.
     */
    Set<String> getChannelIdentifiers();
}
