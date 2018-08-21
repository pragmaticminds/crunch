package org.pragmaticminds.crunch.api.windowed;

import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.filter.EventFilter;
import org.pragmaticminds.crunch.api.windowed.extractor.WindowExtractor;
import org.pragmaticminds.crunch.events.Event;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * This class accumulates all records as long as the window is open (condition is matched)
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class WindowedEvaluationFunction implements EvaluationFunction {
    private RecordWindow    recordWindowPrototype;
    private WindowExtractor extractorPrototype;
    private EventFilter     filterPrototype;
    private RecordWindow    recordWindow;
    private WindowExtractor extractor;
    private EventFilter     filter;
    private boolean         lastWindowOpen = false;
    
    /**
     * Private Constructor for the Builder of this class.
     *
     * @param recordWindow determines whether a window is open or closed.
     * @param extractor extracts resulting {@link Event} {@link java.util.Collection} when the window is closing.
     * @param filter (optional) filters the resulting {@link Event} {@link Collection}, so only relevant {@link Event}s
     *               are passed further.
     */
    private WindowedEvaluationFunction(
        RecordWindow recordWindow,
        WindowExtractor extractor,
        EventFilter filter
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
     * evaluates the incoming {@link MRecord} from the {@link EvaluationContext} and passes the results
     * back to the collect method of the context.
     * Collects all records as long as the window is open. As soon as the window is closed all accumulated records are
     * processed by the {@link WindowExtractor} and then are dropped. On the first window open Signal the
     * accumulation begins from the start.
     *
     * @param context contains incoming data and a collector for the outgoing data
     */
    @Override
    public void eval(EvaluationContext context) {
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
     * Checks if the results are set and if a filter is set, applies the filter on the results and collects them by
     * the context.
     *
     * @param context that collects the final results
     * @param results to be filtered and collected
     */
    private void collectResults(EvaluationContext context, Collection<Event> results) {
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
    private Collection<Event> applyFilter(Collection<Event> results, MRecord record) {
        return results.stream()
            .filter(event -> filter.apply(event, record))
            .collect(Collectors.toList());
    }
    
    /**
     * Creates a builder for this class
     * @return a builder for this class
     */
    public static Builder builder() { return new Builder(); }
    
    /**
     * Builder for this class
     */
    public static final class Builder {
        RecordWindow    recordWindow;
        WindowExtractor extractor;
        EventFilter     filter;
        
        private Builder() {}
        
        public Builder recordWindow(RecordWindow recordWindow) {
            this.recordWindow = recordWindow;
            return this;
        }
        
        public Builder extractor(WindowExtractor extractor) {
            this.extractor = extractor;
            return this;
        }
        
        public Builder filter(EventFilter filter) {
            this.filter = filter;
            return this;
        }
        
        public Builder but() {
            return builder().recordWindow(recordWindow)
                .extractor(extractor)
                .filter(filter);
        }
        
        public WindowedEvaluationFunction build() {
            return new WindowedEvaluationFunction(recordWindow, extractor, filter);
        }
    }
}
