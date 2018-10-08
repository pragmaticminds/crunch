package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;
import java.util.Collection;

/**
 * This class filters incoming {@link MRecord}s by the channel identifiers of all elements inside the {@link SubStream}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.10.2018
 */
public class ChannelFilter<T extends Serializable> implements Serializable {
    private final SubStream<T> subStream;
    
    /**
     * Main constructor taking the SubStream, the source of all filtering operations.
     *
     * @param subStream containing the channel identifiers.
     */
    public ChannelFilter(SubStream<T> subStream) {
        this.subStream = subStream;
    }
    
    /**
     * Filters incoming records. Passes only those which have at least on of the processed channel identifiers from
     * the SubStream.
     *
     * @param record to be filtered out or not
     * @return true if record can pass trough else false
     */
    public boolean filter(MRecord record){
        Collection<String> recordChannels = record.getChannels();
        Collection<String> subStreamChannels = subStream.getChannelIdentifiers();
        for (String recordChannel : recordChannels){
            if(subStreamChannels.contains(recordChannel)){
                return true;
            }
        }
        return false;
    }
}
