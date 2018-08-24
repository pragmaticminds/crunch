package org.pragmaticminds.crunch.api.windowed.extractor;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.api.windowed.extractor.GroupByExtractor.Builder;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;

import java.util.Map;

/**
 * This is the Default to use {@link GroupAggregationFinalizer} implementation, which is used when no finalizer is set
 * in the {@link Builder}. It takes the all aggregatedValues and packs them into the resulting Event by their name.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.08.2018
 */
public class DefaultGroupAggregationFinalizer implements GroupAggregationFinalizer {
    private static int count = 0;
    private int groupNumber;
    
    /**
     * This constructor counts the instances of this class for the naming of the results
     */
    public DefaultGroupAggregationFinalizer() {
        groupNumber = count++;
    }
    
    /**
     * Packs all aggregated values into resulting {@link Event} by their identifier.
     *
     * @param aggregatedValues is a map of all aggregated values, that can be further processed and be added as
     *                         parameters into the resulting {@link Event}s.
     * @param context          current from the evaluation call. Takes the resulting {@link Event}s, with the aggregated values
     */
    @Override
    public void onFinalize(Map<String, Object> aggregatedValues, EvaluationContext context) {
        // if nothing is set -> return empty list
        if(aggregatedValues.isEmpty()){
            return;
        }
    
        // prepare new EventBuilder
        EventBuilder eventBuilder = EventBuilder.anEvent()
            .withEvent(String.format("GROUP_%d", groupNumber))
            .withSource(context.get().getSource())
            .withTimestamp(context.get().getTimestamp());
    
        // add all results as parameters in the new Event
        aggregatedValues.forEach((key, value) -> eventBuilder.withParameter(key, Value.of(value)));
    
        // build the event, pack in list and return
        context.collect(eventBuilder.build());
    }
}
