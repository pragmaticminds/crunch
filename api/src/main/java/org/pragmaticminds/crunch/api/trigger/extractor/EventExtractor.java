package org.pragmaticminds.crunch.api.trigger.extractor;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;

/**
 * Processes triggered {@link TypedValues} and eventually extracts resulting {@link Event}s
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.07.2018
 */
@FunctionalInterface
public interface EventExtractor extends Serializable {

    /**
     * Extract the resulting {@link Event}s after Trigger was activated
     *
     * @param ctx contains the incoming {@link TypedValues} and a collect method to pass resulting {@link Event}s over
     */
    void process(EvaluationContext ctx);
}
