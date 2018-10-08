package org.pragmaticminds.crunch.api.windowed.extractor;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.api.windowed.extractor.GroupByExtractor.Builder;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.GenericEventBuilder;

import java.util.Map;

/**
 * This is the Default to use {@link GroupAggregationFinalizer} implementation, which is used when no finalizer is set
 * in the {@link Builder}. It takes the all aggregatedValues and packs them into the resulting GenericEvent by their name.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.08.2018
 */
public class DefaultGenericEventGroupAggregationFinalizer implements GroupAggregationFinalizer<GenericEvent> {
    private static int count = 0;
    private int groupNumber;
    
    /**
     * This constructor counts the instances of this class for the naming of the results
     */
    public DefaultGenericEventGroupAggregationFinalizer() {
        groupNumber = count++;
    }
    
    /**
     * Packs all aggregated values into resulting T by their identifier.
     *
     * @param aggregatedValues is a map of all aggregated values, that can be further processed and be added as
     *                         parameters into the resulting Ts.
     * @param context          current from the evaluation call. Takes the resulting Ts, with the aggregated values
     */
    @Override
    public void onFinalize(Map<String, Object> aggregatedValues, EvaluationContext<GenericEvent> context) {
        // if nothing is set -> return empty list
        if(aggregatedValues.isEmpty()){
            return;
        }
    
        // prepare new GenericEventBuilder
        GenericEventBuilder eventBuilder = GenericEventBuilder.anEvent()
            .withEvent(String.format("GROUP_%d", groupNumber))
            .withSource(context.get().getSource())
            .withTimestamp(context.get().getTimestamp());
    
        // add all results as parameters in the new GenericEvent
        aggregatedValues.forEach((key, value) -> {
            if(value != null){
                eventBuilder.withParameter(key, Value.of(value));
            }
        });
    
        // build the event, pack in list and return
        context.collect(eventBuilder.build());
    }
}
