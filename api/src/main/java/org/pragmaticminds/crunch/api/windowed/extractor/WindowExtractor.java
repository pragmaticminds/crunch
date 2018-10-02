package org.pragmaticminds.crunch.api.windowed.extractor;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.windowed.WindowedEvaluationFunction;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;
import java.util.Collection;

/**
 * Extracts resulting {@link GenericEvent}s from {@link MRecord}s for
 * {@link EvaluationFunction}s which are using this interface.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public interface WindowExtractor<T extends Serializable> extends Serializable {
    
    /**
     * This method collects single values for the later made extraction.
     * @param record from the eval call of the {@link WindowedEvaluationFunction}.
     */
    void apply(MRecord record);
    
    /**
     * Generates resulting {@link Collection} of {@link GenericEvent} from the applied {@link MRecord}s and
     * an {@link EvaluationContext} after am {@link EvaluationFunction} has met it's entry conditions.
     * @param context of the current eval call to the parent {@link EvaluationFunction}. also collects the resulting
     *                {@link GenericEvent}s
     */
    void finish(EvaluationContext<T> context);
}
