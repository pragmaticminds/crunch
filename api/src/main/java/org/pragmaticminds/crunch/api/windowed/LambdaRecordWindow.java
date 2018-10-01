package org.pragmaticminds.crunch.api.windowed;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.SerializableFunction;
import org.pragmaticminds.crunch.api.trigger.comparator.SerializableResultFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Wraps the {@link RecordWindow} interface so that it can be implemented with lambdas.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class LambdaRecordWindow implements RecordWindow {
    
    private SerializableFunction<MRecord, Boolean> inWindowLambda;
    private SerializableResultFunction<ArrayList<String>> getChannelIdentifiers;
    
    public LambdaRecordWindow(
        SerializableFunction<MRecord, Boolean> inWindowLambda,
        SerializableResultFunction<ArrayList<String>> getChannelIdentifiers
    ) {
        this.inWindowLambda = inWindowLambda;
        this.getChannelIdentifiers = getChannelIdentifiers;
    }
    
    /**
     * Checks if a processing window is open or closed.
     * In case of open -> all records will be accumulated in the next processing instance of the record.
     * In case of closed -> all records and the current are dropped in the next processing instance of the record.
     *
     * @param record is checked if a window is open or closed
     * @return true if window is open, false otherwise
     */
    @Override
    public boolean inWindow(MRecord record) {
        return inWindowLambda.apply(record);
    }
    
    /**
     * Collects all identifiers of channels that are used in here.
     *
     * @return a {@link List} or {@link Collection} of channel identifiers.
     */
    @Override
    public Collection<String> getChannelIdentifiers() {
        return getChannelIdentifiers.get();
    }
}
