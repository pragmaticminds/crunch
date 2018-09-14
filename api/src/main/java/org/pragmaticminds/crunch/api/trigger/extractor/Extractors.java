package org.pragmaticminds.crunch.api.trigger.extractor;

import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.trigger.comparator.Suppliers;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;

import java.util.*;
import java.util.stream.Collector;
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
     * !!! This method was replaced by the method:
     *    public static EventExtractor valuesExtractor(String eventName, String... channels).
     *
     * @param channels channelNames to be extracted as parameter names for the {@link Event}.
     * @return an {@link EventExtractor}.
     *
     * @deprecated this method is obsolete, use the one with event name as extra parameter !!!
     */
    @Deprecated
    public static EventExtractor valuesExtractor(String... channels){
        return valuesExtractor(
            "Unknown",
            Arrays.stream(channels)
                    .map(Suppliers.ChannelExtractors::channel)
                .collect(Collectors.toList())
                .toArray(new Supplier[channels.length])
        );
    }
    
    /**
     * Creates an {@link EventExtractor}, which collects the given channels by their names
     * @param channelSuppliers to be collected as parameters for the resulting Event
     * @return an {@link EventExtractor} that collects the given channels by their names and inserting them in a
     *         resulting {@link Event}
     */
    public static <T> EventExtractor valuesExtractor(String eventName, Supplier<T>... channelSuppliers){
        return ctx -> {
            // create a map of parameters for the event to be created
            Map<String, Value> collectedValues = Arrays.stream(channelSuppliers)
                .collect(
                    // supplier
                    HashMap::new,
                    // accumulator
                    (map, channelSupplier) ->
                        map.put(
                            channelSupplier.getIdentifier(),
                            Value.of(channelSupplier.extract(ctx.get()))
                        ),
                    // combiner
                    HashMap::putAll
                );
            
            // create Event and put in a ArrayList
            return new ArrayList<>(Collections.singletonList(
                EventBuilder.anEvent()
                    .withEvent(eventName)
                    .withTimestamp(ctx.get().getTimestamp())
                    .withSource(ctx.get().getSource())
                    .withParameters(collectedValues)
                    .build()
            ));
        };
    }
    
    /**
     * !!! this method is obsolete !!! use the other method with event name as parameter !!!
     * Creates an {@link EventExtractor}, which collects the given channels by their names
     *
     * @param aliasedChannels a Map of the channel names and the names they should get, when they are stored in the
     *                        {@link Event}.
     * @return an {@link EventExtractor} that collects the given channels by their names and inserting them in a
     *         resulting {@link Event} with their alias names
     * @deprecated this method is obsolete, use the one with evant name as parameter.
     */
    @Deprecated
    public static EventExtractor valuesExtractor(Map<String, String> aliasedChannels){
        Collector<Map.Entry<String, String>, HashMap<Supplier, String>, HashMap<Supplier, String>> collector =
            Collector.of(
                // collector supplier
                HashMap::new,
                // collector accumulator
                    (map, entry) -> map.put(Suppliers.ChannelExtractors.channel(entry.getKey()), entry.getValue()),
                // collector combiner
                (map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                }
            );
        Map<Supplier, String> mapping = aliasedChannels.entrySet().stream().collect(collector);
        return valuesExtractor(
            "Unknown", mapping
        );
    }
    
    /**
     * Creates an {@link EventExtractor}, which collects the given channels by their names
     *
     * @param aliasedChannels a Map of the channel names and the names they should get, when they are stored in the
     *                        {@link Event}.
     * @return an {@link EventExtractor} that collects the given channels by their names and inserting them in a
     *         resulting {@link Event} with their alias names
     */
    public static EventExtractor valuesExtractor(String eventName, Map<Supplier, String> aliasedChannels){
        return ctx -> {
            // collect the channel names of interest and save them under the alias key
            Map<String, Value> collectedValues = aliasedChannels.entrySet()
                .stream()
                .collect(
                    // supplier
                    HashMap::new,
                    // accumulator
                    (map, entry) -> map.put(
                        // get value as new key
                        entry.getValue(),
                        // get value of filed in MRecord by the key
                        Value.of(entry.getKey().extract(ctx.get()))
                    ),
                    // combiner
                    HashMap::putAll
                );
    
            // create Event and put in a ArrayList
            return new ArrayList<>(Collections.singletonList(
                EventBuilder.anEvent()
                    .withEvent(eventName)
                    .withTimestamp(ctx.get().getTimestamp())
                    .withSource(ctx.get().getSource())
                    .withParameters(collectedValues)
                    .build()
            ));
        };
    }
    
    /**
     * Creates an {@link EventExtractor}, which collects all channel values from an MRecord.
     *
     * @param eventName Name of the {@link Event} that is created inside this method.
     * @return the resulting {@link EventExtractor}.
     */
    public static EventExtractor allValuesExtractor(String eventName){
        return ctx -> new ArrayList<>(Collections.singletonList(
            // build an event
            EventBuilder.anEvent()
                // set event name
                .withEvent(eventName)
                // set source
                .withSource(ctx.get().getSource())
                // set timestamp
                .withTimestamp(ctx.get().getTimestamp())
                // set parameters
                .withParameters(
                    // extract all channel values as Map
                    ctx.get()
                        // stream all channel names
                        .getChannels().stream()
                        // collect as map
                        .collect(
                            Collectors.toMap(
                                // channel name as channel name
                                channel -> channel,
                                // extract the Value from the record and put it into the map
                                channel -> ctx.get().getValue(channel)
                            )
                        )
                )
                .build()
        ));
    }
}
