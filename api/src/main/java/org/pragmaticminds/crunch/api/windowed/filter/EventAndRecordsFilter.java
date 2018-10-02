package org.pragmaticminds.crunch.api.windowed.filter;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Filters Ts by itself and the {@link MRecord} {@link java.util.List} that was the base for the creation
 * of the T.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public interface EventAndRecordsFilter<T extends Serializable> extends Serializable {
    
    /**
     * Filters all Events that should not be passed to further processing.
     * The filtering is based on the T itself and on the {@link MRecord} {@link java.util.List} to
     * determine if an T is to be passed further.
     *
     * @param event of interest, that should be filtered or not.
     * @param records that were used to create the T.
     * @return true if the T is to be send further for processing, otherwise false.
     */
    boolean apply(T event, Collection<MRecord> records);
    
    /**
     * Collects all channel identifiers that are used in this filter.
     *
     * @return a {@link List} or {@link Collection} with all channels that are used to filter.
     */
    Collection<String> getChannelIdentifiers();
}
