package org.pragmaticminds.crunch.api.trigger.extractor;

import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A collection of {@link EventExtractor} implementation for usual use cases
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 14.08.2018
 */
public class Extractors {
    private Extractors() {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Creates an {@link EventExtractor}, which collects the given channels by their names
     * @param channels to be collected as parameters for the resulting Event
     * @return an {@link EventExtractor} that collects the given channels by their names and inserting them in a
     *         resulting {@link Event}
     */
    public static EventExtractor valuesExtractor(String... channels){
        return ctx -> {
            // create a map of parameters for the event to be created
            Map<String, Value> collectedValues = Arrays.stream(channels)
                .collect(Collectors.toMap(
                    channel -> channel,
                    channel -> ctx.get().getValue(channel)
                ));
            
            // create Event and put in a ArrayList
            return new ArrayList<>(Collections.singletonList(
                EventBuilder.anEvent()
                    .withEvent("values")
                    .withTimestamp(ctx.get().getTimestamp())
                    .withSource(ctx.get().getSource())
                    .withParameters(collectedValues)
                    .build()
            ));
        };
    }
    
    /**
     * Creates an {@link EventExtractor}, which collects the given channels by their names
     *
     * @param aliasedChannels a Map of the channel names and the names they should get, when they are stored in the
     *                        {@link Event}.
     * @return an {@link EventExtractor} that collects the given channels by their names and inserting them in a
     *         resulting {@link Event} with their alias names
     */
    public static EventExtractor valuesExtractor(Map<String, String> aliasedChannels){
        return ctx -> {
            // collect the channel names of interest and save them under the alias key
            Map<String, Value> collectedValues = aliasedChannels.entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getValue,
                        entry -> ctx.get().getValue(entry.getKey())
                    )
                );
    
            // create Event and put in a ArrayList
            return new ArrayList<>(Collections.singletonList(
                EventBuilder.anEvent()
                    .withEvent("values")
                    .withTimestamp(ctx.get().getTimestamp())
                    .withSource(ctx.get().getSource())
                    .withParameters(collectedValues)
                    .build()
            ));
        };
    }
}
