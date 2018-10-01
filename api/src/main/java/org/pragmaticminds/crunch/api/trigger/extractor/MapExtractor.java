package org.pragmaticminds.crunch.api.trigger.extractor;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.io.Serializable;
import java.util.Map;

/**
 * This class extracts a {@link Map} of keyed {@link Value}s from a {@link EvaluationContext}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.09.2018
 */
@FunctionalInterface
public interface MapExtractor extends Serializable {
    /**
     * This method extracts a map of {@link Value}s from a {@link EvaluationContext}, in particular from it's
     * {@link MRecord}.
     *
     * @param context the current {@link EvaluationContext} that holds the current {@link MRecord}.
     * @return a {@link Map} of keyed extracted values from the {@link EvaluationContext}s {@link MRecord}.
     */
    Map<String, Value> extract(EvaluationContext context);
}
