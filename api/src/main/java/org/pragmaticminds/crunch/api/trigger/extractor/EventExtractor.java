package org.pragmaticminds.crunch.api.trigger.extractor;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;
import java.util.Collection;

/**
 * Processes triggered {@link MRecord} and eventually extracts resulting {@link Event}s
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.07.2018
 */
@FunctionalInterface
public interface EventExtractor extends Serializable {
    
    /**
     * Extract the resulting {@link Event}s after Trigger was activated
     * @param ctx contains the incoming {@link MRecord}s
     * @return ArrayList of resulting {@link Event}s
     */
    Collection<Event> process(EvaluationContext ctx);
}
