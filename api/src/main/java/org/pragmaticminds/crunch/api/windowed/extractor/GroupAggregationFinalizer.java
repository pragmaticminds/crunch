package org.pragmaticminds.crunch.api.windowed.extractor;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;
import java.util.Map;

/**
 * This represents the last step of processing in a {@link GroupByExtractor}. This class gets a map of named resulting
 * values. With this {@link Map} this class creates resulting {@link Event}s which than go out for processing of the
 * next steps.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
@FunctionalInterface
public interface GroupAggregationFinalizer extends Serializable {
    
    /**
     * Packs the aggregated values into resulting {@link Event}s.
     *
     * @param aggregatedValues is a map of all aggregated values, that can be further processed and be added as
     *                         parameters into the resulting {@link Event}s.
     * @param context current from the evaluation call. Takes the resulting {@link Event}s, with the aggregated values
     *                as parameters.
     */
    void onFinalize(Map<String, Object> aggregatedValues, EvaluationContext context);
}
