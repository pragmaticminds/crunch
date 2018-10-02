package org.pragmaticminds.crunch.api.windowed;

import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.filter.EventFilter;
import org.pragmaticminds.crunch.api.windowed.extractor.WindowExtractor;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class accumulates all records as long as the window is open (condition is matched)
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class WindowedEvaluationFunction<T extends Serializable> implements EvaluationFunction<T> {
    private RecordWindow    recordWindowPrototype;
    private WindowExtractor<T> extractorPrototype;
    private EventFilter<T> filterPrototype;
    private RecordWindow    recordWindow;
    private WindowExtractor<T> extractor;
    private EventFilter<T> filter;
    private boolean         lastWindowOpen = false;
    
    /**
     * Private Constructor for the Builder of this class.
     *
     * @param recordWindow determines whether a window is open or closed.
     * @param extractor extracts resulting {@link GenericEvent} {@link java.util.Collection} when the window is closing.
     * @param filter (optional) filters the resulting {@link GenericEvent} {@link Collection}, so only relevant {@link GenericEvent}s
     *               are passed further.
     */
    private WindowedEvaluationFunction(
            RecordWindow recordWindow,
            WindowExtractor<T> extractor,
            EventFilter<T> filter
    ) {
        this.recordWindowPrototype = recordWindow;
        this.extractorPrototype = extractor;
        this.filterPrototype = filter;
        
        makeLocalInstances();
        
        checkValues();
    }
    
    /** Check if all necessary members are available */
    private void checkValues() {
        Preconditions.checkNotNull(recordWindowPrototype);
        Preconditions.checkNotNull(extractorPrototype);
    }
    
    private void makeLocalInstances(){
        recordWindow = ClonerUtil.clone(recordWindowPrototype);
        extractor = ClonerUtil.clone(extractorPrototype);
        if(filterPrototype != null){
            filter = ClonerUtil.clone(filterPrototype);
        }
    }

    /**
     * Creates a builder for this class
     *
     * @return a builder for this class
     */
    public static <B extends Serializable> Builder<B> builder() {
        return new Builder<>();
    }
    
    /**
     * evaluates the incoming {@link MRecord} from the {@link EvaluationContext} and passes the results
     * back to the collect method of the context.
     * Collects all records as long as the window is open. As soon as the window is closed all accumulated records are
     * processed by the {@link WindowExtractor} and then are dropped. On the first window open Signal the
     * accumulation begins from the start.
     *
     * @param context contains incoming data and a collector for the outgoing data
     */
    @Override
    public void eval(EvaluationContext<T> context) {
        // as long as the window is open -> collect all records
        if(recordWindow.inWindow(context.get())){
            lastWindowOpen = true;
            extractor.apply(context.get());
        } else {
            // if not first time window is closed -> return
            if (!lastWindowOpen) {
                return;
            }

            SimpleEvaluationContext simpleContext = new SimpleEvaluationContext(context.get());

            // extract the resulting events
            extractor.finish(simpleContext);

            // filter results and call collect on each
            collectResults(context, simpleContext.getEvents());

            // recreate local instances to set everything on the start conditions
            makeLocalInstances();

            // set last window was closed
            lastWindowOpen = false;
        }
    }
    
    /**
     * Collects all channel identifiers, that are used for the triggering condition
     *
     * @return a {@link Set} of all channel identifiers from triggering
     */
    @Override
    public Set<String> getChannelIdentifiers() {
        Set<String> results = new HashSet<>();
        if(filterPrototype != null){
            results.addAll(filterPrototype.getChannelIdentifiers());
        }
        results.addAll(recordWindowPrototype.getChannelIdentifiers());
        return results;
    }
    
    /**
     * Checks if the results are set and if a filter is set, applies the filter on the results and collects them by
     * the context.
     *
     * @param context that collects the final results
     * @param results to be filtered and collected
     */
    private void collectResults(EvaluationContext<T> context, Collection<T> results) {
        if(results != null){
            if(filter != null){
                applyFilter(results, context.get())
                    .forEach(context::collect);
            }else{
                results.forEach(context::collect);
            }
        }
    }
    
    /**
     * applies the filter on the results
     * @param results to be filtered
     * @return the filtered results
     */
    private Collection<T> applyFilter(Collection<T> results, MRecord record) {
        return results.stream()
            .filter(event -> filter.apply(event, record))
            .collect(Collectors.toList());
    }
    
    /**
     * Builder for this class
     */
    public static final class Builder<T extends Serializable> {
        RecordWindow    recordWindow;
        WindowExtractor<T> extractor;
        EventFilter<T> filter;
        
        private Builder() {}

        public Builder<T> recordWindow(RecordWindow recordWindow) {
            this.recordWindow = recordWindow;
            return this;
        }

        public Builder<T> extractor(WindowExtractor<T> extractor) {
            this.extractor = extractor;
            return this;
        }

        public Builder<T> filter(EventFilter<T> filter) {
            this.filter = filter;
            return this;
        }

        public Builder<T> but() {
            return (new Builder<T>()).recordWindow(recordWindow)
                .extractor(extractor)
                .filter(filter);
        }

        public WindowedEvaluationFunction<T> build() {
            return new WindowedEvaluationFunction<>(recordWindow, extractor, filter);
        }
    }
}
