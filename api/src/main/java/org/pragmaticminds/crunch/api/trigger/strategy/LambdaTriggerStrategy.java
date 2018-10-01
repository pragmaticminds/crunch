package org.pragmaticminds.crunch.api.trigger.strategy;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.SerializableFunction;
import org.pragmaticminds.crunch.api.trigger.comparator.SerializableResultFunction;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps the {@link TriggerStrategy} so that it can be implemented with lambda definitions.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class LambdaTriggerStrategy implements TriggerStrategy {
    
    private SerializableFunction<MRecord, Boolean> isToBeTriggeredLambda;
    private SerializableResultFunction<HashSet<String>> getChannelIdentifiersLambda;
    
    /**
     * Main constructor that takes the lambdas which are defining the processing.
     *
     * @param isToBeTriggeredLambda       determines whether to be triggered.
     * @param getChannelIdentifiersLambda collect all channel identifiers that are being used.
     */
    public LambdaTriggerStrategy(
            SerializableFunction<MRecord, Boolean> isToBeTriggeredLambda,
            SerializableResultFunction<HashSet<String>> getChannelIdentifiersLambda) {
        this.isToBeTriggeredLambda = isToBeTriggeredLambda;
        this.getChannelIdentifiersLambda = getChannelIdentifiersLambda;
    }
    
    /**
     * Decides if the event is to be triggered by the decision base typed value
     *
     * @param values contains the indicator if processing is to be triggered
     * @return true if triggering was positive
     */
    @Override
    public boolean isToBeTriggered(MRecord values) {
        return isToBeTriggeredLambda.apply(values);
    }

    /**
     * Returns all channel identifiers which are necessary for the function to do its job.
     * It is not allowed to return null, an empty set can be returned (but why should??).
     *
     * @return a {@link Set} all channel identifiers that are needed by the Evaluation Function.
     */
    @Override
    public Set<String> getChannelIdentifiers() {
        return getChannelIdentifiersLambda.get();
    }
}
