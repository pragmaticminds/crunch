package org.pragmaticminds.crunch.api.trigger.comparator;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * This is a abstract implementation of the {@link Supplier}, where the setting of the identifier is handled.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.08.2018
 */
class NamedSupplier<T extends Serializable> implements Supplier<T> {
    private String identifier;
    private SerializableFunction<MRecord, T> extractLambda;
    private SerializableResultFunction<HashSet<String>> getIdentifiersLambda;
    
    /**
     * Main constructor with identifier
     *
     * @param identifier identifies this {@link Supplier} implementation
     * @param extractLambda extracts the value of interest from the {@link MRecord}
     */
    @SuppressWarnings("unchecked") // is insured to be safe
    public NamedSupplier(
            String identifier,
            SerializableFunction<MRecord, T> extractLambda,
            SerializableResultFunction<HashSet<String>> getIdentifiersLambda
    ) {
        this.identifier = identifier;
        this.extractLambda = extractLambda;
        this.getIdentifiersLambda = getIdentifiersLambda;
    }
    
    /**
     * Compares the incoming values with internal criteria and returns a result of T
     *
     * @param values incoming values to be compared to internal criteria
     * @return a result of T
     */
    @Override
    public T extract(MRecord values) {
        return extractLambda.apply(values);
    }
    
    /**
     * All suppliers have to be identifiable
     *
     * @return String identifier of the {@link Supplier} implementation
     */
    @Override
    public String getIdentifier() {
        return identifier;
    }
    
    /** @inheritDoc */
    @Override
    public Set<String> getChannelIdentifiers() {
        return getIdentifiersLambda.get();
    }
}
