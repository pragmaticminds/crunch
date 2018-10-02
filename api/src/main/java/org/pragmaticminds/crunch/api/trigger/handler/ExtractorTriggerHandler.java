package org.pragmaticminds.crunch.api.trigger.handler;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.TriggerEvaluationFunction;
import org.pragmaticminds.crunch.api.trigger.extractor.MapExtractor;
import org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategy;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles the extraction of the resulting Event, when a {@link TriggerStrategy} in a {@link TriggerEvaluationFunction}
 * was triggered.
 * This implementation of the TriggerHandler holds a {@link List} of {@link MapExtractor}s, which are extracting
 * {@link Value}s of interest from an {@link MRecord}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.09.2018
 */
public abstract class ExtractorTriggerHandler<T extends Serializable> implements TriggerHandler<T> {
    private String eventName;
    private ArrayList<MapExtractor> extractors = null;

    /**
     * Constructor with array of {@link MapExtractor}s.
     *
     * @param eventName Name of the resulting {@link Event}s.
     * @param extractors Array of extractors, which delivers the parameters for the resulting {@link Event}s.
     */
    public ExtractorTriggerHandler(String eventName, MapExtractor... extractors) {
        this.eventName = eventName;
        if(extractors != null && extractors.length != 0){
            this.extractors = new ArrayList<>(Arrays.asList(extractors));
        }
    }

    /**
     * Constructor with {@link Collection}/{@link List} of {@link MapExtractor}s.
     *
     * @param eventName Name of the resulting {@link Event}s.
     * @param extractors {@link Collection}/{@link List} of extractors, which delivers the parameters for the
     *                   resulting {@link Event}s.
     */
    public ExtractorTriggerHandler(String eventName, Collection<MapExtractor> extractors) {
        this.eventName = eventName;
        if(extractors == null){
            return;
        }
        this.extractors = new ArrayList<>(extractors);
    }

    /**
     * When a {@link TriggerEvaluationFunction} is triggered, it calls this method to generate a proper result.
     *
     * @param context of the current processing. It holds the current MRecord and it takes the resulting {@link Event}
     *                objects.
     */
    @Override
    public void handle(EvaluationContext<T> context) {
        // when nothing is set -> no results
        if(extractors == null || extractors.isEmpty()){
            return;
        }

        // merge the results of all extractors into one map
        Map<String, Value> results = extractors.stream()
            // let all extractors extract their result data
            .flatMap(
                extractor ->
                    extractor
                        .extract(context)
                        .entrySet()
                        .stream()
            )
            // combine all maps into one
            .collect(
                Collectors.toMap(
                    Map.Entry<String, Value>::getKey,
                    Map.Entry<String, Value>::getValue
                )
            );

        // collect the resulting Event with the context
        context.collect(
            createEvent(eventName, context, results)
        );
    }
    
    protected abstract T createEvent(String eventName, EvaluationContext<T> context, Map<String, Value> results);
}
