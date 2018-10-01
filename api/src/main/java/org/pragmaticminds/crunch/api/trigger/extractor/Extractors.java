package org.pragmaticminds.crunch.api.trigger.extractor;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Collection;
import java.util.Map;

/**
 * This class holds all available {@link MapExtractor} implementations.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.09.2018
 */
public class Extractors {
    /** hidden constructor */
    private Extractors() {
        throw new UnsupportedOperationException("this constructor should never be used!");
    }
    
    /**
     * Creates a {@link MapExtractor} that collects the {@link Value}s of all available channels in a {@link MRecord}.
     *
     * @return a new instance of the {@link AllChannelMapExtractor}.
     */
    public static AllChannelMapExtractor allChannelMapExtractor(){
        return new AllChannelMapExtractor();
    }
    
    /**
     * Creates a {@link MapExtractor} that collects the {@link Value}s of a all {@link Supplier}s in the {@link Map},
     * which are saved in the resulting {@link Map} by their given {@link String} mapping name in the {@link Map}.
     *
     * @param mapping Holds a {@link Map} of {@link Supplier} for a {@link Value} to its mapped name.
     * @return a new instance of the {@link ChannelMapExtractor}.
     */
    public static ChannelMapExtractor channelMapExtractor(Map<Supplier, String> mapping){
        return new ChannelMapExtractor(mapping);
    }
    
    /**
     * Creates a {@link MapExtractor} that collects the {@link Value}s of a all {@link Supplier}s in the {@link Map},
     * which are saved in the resulting {@link Map} by the identifier of the {@link Supplier}, which is the name of
     * the channel in the MRecord.
     *
     * @param channels Holds a {@link Collection} of {@link Supplier} for {@link Value}s.
     * @return a new instance of the {@link ChannelMapExtractor}.
     */
    public static ChannelMapExtractor channelMapExtractor(Collection<Supplier> channels){
        return new ChannelMapExtractor(channels);
    }
    
    /**
     * Creates a {@link MapExtractor} that collects the {@link Value}s of a all {@link Supplier}s in the {@link Map},
     * which are saved in the resulting {@link Map} by the identifier of the {@link Supplier}, which is the name of
     * the channel in the MRecord.
     *
     * @param channels Holds an array of {@link Supplier} for {@link Value}s.
     * @return a new instance of the {@link ChannelMapExtractor}.
     */
    public static ChannelMapExtractor channelMapExtractor(Supplier... channels){
        return new ChannelMapExtractor(channels);
    }
}