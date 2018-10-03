package org.pragmaticminds.crunch.execution;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.security.InvalidParameterException;

/**
 * This class implements the interface {@link MergeFunction}.
 * It takes {@link UntypedValues} or {@link UntypedValues} as incoming values and produces outgoing {@link UntypedValues}.
 * It combines the values of the internal {@link UntypedValues} with the incoming {@link MRecord} values and returns
 * the result in the {@link #merge(MRecord)} method.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 02.10.2018
 */
public class UntypedValuesMergeFunction implements MergeFunction<MRecord, MRecord> {
    
    private UntypedValues values = null;
    
    /**
     * Merges incomming values to a aggregated value containing all other sub values from before.
     *
     * @param currentValue to be merged
     * @return merged value
     */
    @Override
    public MRecord merge(MRecord currentValue) {
        if(!UntypedValues.class.isInstance(currentValue)){
            throw new InvalidParameterException(String.format(
                "ValuesMergeFunction currently only supports UntypedValues and not %s",
                currentValue.getClass().getName()
            ));
        }
        
        // Do the mapping
        values = mapWithoutState(values, currentValue);
        return values;
    }
    
    /**
     * Internal method that does the merging of the state.
     * Does not fetch / rewrite the Function's state.
     *
     * @param currentValues the inner values that is the merge from all calls before {@link MRecord}s.
     * @param newValues the value from the current call tho the {@link #merge(MRecord)} method.
     * @return the merged {@link UntypedValues} object from the inner #currentValues and the incoming {@link UntypedValues}
     */
    UntypedValues mapWithoutState(UntypedValues currentValues, MRecord newValues) {
        // Init the valueState object on first value object
        UntypedValues state;
        if (currentValues == null) {
            state = (UntypedValues) newValues;
        } else {
            state = currentValues.merge((UntypedValues) newValues);
        }
        return state;
    }
}
