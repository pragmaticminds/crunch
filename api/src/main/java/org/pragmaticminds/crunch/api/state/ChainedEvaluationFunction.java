package org.pragmaticminds.crunch.api.state;

import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.timer.ReferenceTimer;
import org.pragmaticminds.crunch.api.pipe.timer.Timer;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.events.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This implementation of the {@link EvaluationFunction} represents a linear statemachine. All inner
 * {@link EvaluationFunction}s must be processed
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public class ChainedEvaluationFunction implements EvaluationFunction {
    public static final  String TIMEOUT = "timeout";
    
    private static final Logger logger  = LoggerFactory.getLogger(ChainedEvaluationFunction.class);

    private final List<StateConfig> stateConfigs;
    private final Long                                                            overallTimeoutMs;
    
    private StateErrorExtractor              errorExtractor;
    private ChainProcessingCompleteExtractor stateCompleteExtractor;
    private Timer                            stateTimer;
    private Timer                            overallTimer;
    private int                              currentStep = 0;
    private EvaluationFunction               currentStateEvaluationFunction;
    private StateEvaluationContext           innerContext;
    
    /**
     * Main constructor of this class for the Builder.
     *
     * @param stateConfigs is a list of {@link StateConfig} values, which contains a {@link EvaluationFunction} factory
     *                                  and a timeout for the chain step
     * @param overallTimeoutMs a timeout value for the complete processing duration of the processing of all inner
     *                         {@link EvaluationFunction}s
     * @param errorExtractor is a construct, that generates an error {@link Event} if a timeout should been raised
     * @param stateCompleteExtractor is a construct, that evaluates all inner {@link EvaluationFunction}s results to
     *                               generate final {@link Event}s to be send out of the {@link ChainedEvaluationFunction}
     */
    private ChainedEvaluationFunction(
            List<StateConfig> stateConfigs,
            Long overallTimeoutMs,
            StateErrorExtractor errorExtractor,
            ChainProcessingCompleteExtractor stateCompleteExtractor
    ) {
        // Check Preconditions
        Preconditions.checkNotNull(stateConfigs);
        Preconditions.checkNotNull(errorExtractor);
        Preconditions.checkNotNull(stateCompleteExtractor);

        this.stateConfigs = new ArrayList<>(stateConfigs);
        this.overallTimeoutMs = overallTimeoutMs;
        this.errorExtractor = errorExtractor;
        this.stateCompleteExtractor = stateCompleteExtractor;
        
        this.currentStep = 0;
        this.currentStateEvaluationFunction = stateConfigs.get(currentStep).create();
    }
    
    /**
     * evaluates the incoming {@link TypedValues} from the {@link EvaluationContext} and passes the results
     * back to the collect method of the context
     *
     * @param context contains incoming data and a collector for the outgoing data
     */
    @Override
    public void eval(EvaluationContext context) {
        try{
            // tick on timers if present
            tickOnTimers(context);
    
            // check if timeout appeared
            checkIfTimeoutAppeared(context);
    
            // update or set inner context
            updateOrSetInnerContext(context);
            
            // start the timers if first run after reset
            startTheTimersIfNecessary(context);
    
            // execute the EvaluationFunction
            currentStateEvaluationFunction.eval(innerContext);
            
            // check for resulting Events
            Map<String, Event> events = innerContext.getEvents();
            if(!events.isEmpty()){
                nextState(events, context);
            }
        } catch (Exception ex) { // on thread interrupt
            logger.info("ran into an Exception", ex);
            errorExtractor.process(innerContext == null? Collections.emptyMap() : innerContext.getEvents(), ex, context);
            resetStatemachine();
        }
    }
    
    private void startTheTimersIfNecessary(EvaluationContext context) {
        // start overall timer if running for the first time
        if(overallTimeoutMs != null && overallTimer == null){
            overallTimer = context.createNewTimer(this);
            resetOverallTimeout(overallTimeoutMs);
            overallTimer.tick(context.get().getTimestamp()); // if reference based timer -> needs to tick
        }
        // start state timer if running for the first time
        if (stateTimer == null) {
            stateTimer = context.createNewTimer(this);
            resetStateTimeout(stateConfigs.get(currentStep).getStateTimeout());
            stateTimer.tick(context.get().getTimestamp()); // if reference based timer -> needs to tick
        }
    }
    
    /**
     * Checks if a timeout Event already appeared. If so resets the processing.
     * @param context current of the eval method
     */
    private void checkIfTimeoutAppeared(EvaluationContext context) {
        if(innerContext != null
           && !innerContext.getEvents().isEmpty()
           && innerContext.getEvents().containsKey(TIMEOUT)
        ){
            errorExtractor.process(innerContext.getEvents(), null, context);
            resetStatemachine();
        }
    }
    
    /**
     * Updates the innerContext depending on it has already been initialized
     * @param context current of the eval method
     */
    private void updateOrSetInnerContext(EvaluationContext context) {
        if(innerContext == null){
            innerContext = new StateEvaluationContext(context.get(), stateConfigs.get(currentStep).getStateAlias()) {
                @Override
                public Timer createNewTimer(EvaluationFunction evaluationFunction) {
                    return context.createNewTimer(ChainedEvaluationFunction.this);
                }
            };
        }else{
            // only set the values
            innerContext.set(context.get());
        }
    }
    
    /**
     * If {@link ReferenceTimer}s are used, they need to be ticked
     * @param context current of the eval method
     */
    private void tickOnTimers(EvaluationContext context) {
        if(innerContext != null){
            if(overallTimer != null){
                overallTimer.tick(context.get().getTimestamp());
            }
            if (stateTimer != null){
                stateTimer.tick(context.get().getTimestamp());
            }
        }
    }
    
    /**
     * Switches the chain to the next step, cancels if error timeout occurred or restarts if processing of the chain is
     * complete. In the last case the stateCompleteExtractor is called.
     * @param events so far gained from the processing of the chain steps
     * @param context current of the eval method
     */
    private void nextState(Map<String, Event> events, EvaluationContext context) {
        // stop last timer if present
        if(stateTimer != null){
            stateTimer.cancel();
        }
        
        // if timeout raised while processing
        if (currentStep == stateConfigs.size() - 1) {
            stateCompleteExtractor.process(events, context);
            resetStatemachine();
        } else {
            currentStep++;
            currentStateEvaluationFunction = stateConfigs.get(currentStep).create();
            this.resetStateTimeout(stateConfigs.get(currentStep).getStateTimeout());
        }
    }
    
    /**
     * resets the inner structures in this instance, to begin processing from the start
     */
    private void resetStatemachine() {
        // restart the processing
        currentStep = 0;
        currentStateEvaluationFunction = stateConfigs.get(currentStep).create();
        
        // reset overall timeout and state timeout
        this.resetStateTimeout(stateConfigs.get(currentStep).getStateTimeout());
        resetOverallTimeout(overallTimeoutMs);
        
        innerContext = null;
    }
    
    /**
     * Starts the state timeout if set
     * @param stateTimeoutMs the timeout value for the state
     */
    private void resetStateTimeout(Long stateTimeoutMs) {
        if(stateTimer != null){
            stateTimer.startTimeout(stateTimeoutMs);
        }
    }
    
    /**
     * Starts the overall timeout if set
     * @param overallTimeoutMs the timeout value for the complete processing chain
     */
    private void resetOverallTimeout(Long overallTimeoutMs) {
        if(overallTimer != null){
            overallTimer.startTimeout(overallTimeoutMs);
        }
    }
    
    /**
     * For those {@link EvaluationFunction} that have a timeout
     */
    public void onTimeout() {
        if(innerContext == null || innerContext.get() == null){
            return;
        }
        Event timeout = innerContext.getEventBuilder()
            .withSource(innerContext.get().getSource())
            .withTimestamp(innerContext.get().getTimestamp())
            .withParameter("error_reason", TIMEOUT)
            .build();
        innerContext.collect(TIMEOUT, timeout);
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

        private static final long DEFAULT_TIMEOUT_MS = 3_600_000;

        private List<StateConfig> stateConfigs;
        private long overallTimeoutMs = DEFAULT_TIMEOUT_MS;
        private StateErrorExtractor errorExtractor;
        private ChainProcessingCompleteExtractor stateCompleteExtractor;
        
        private Builder() {}

        /**
         * Adds an EvaluationFunction to the chain of {@link EvaluationFunction}s.
         *
         * @param function the {@link EvaluationFunction} that is added.
         * @param alias the alias for the results of the {@link EvaluationFunction}.
         * @return the {@link Builder}.
         */
        public Builder addEvaluationFunction(EvaluationFunction function, String alias) {
            return addEvaluationFunction(function, alias, DEFAULT_TIMEOUT_MS);
        }

        /**
         * Adds an EvaluationFunction to the chain of {@link EvaluationFunction}s.
         *
         * @param function the {@link EvaluationFunction} that is added.
         * @param alias the alias for the results of the {@link EvaluationFunction}.
         * @param timeoutMs the chain step timeout.
         * @return the {@link Builder}.
         */
        public Builder addEvaluationFunction(EvaluationFunction function, String alias, long timeoutMs) {
            CloneStateEvaluationFunctionFactory factory = CloneStateEvaluationFunctionFactory.builder()
                .withPrototype(function)
                .build();
            return addEvaluationFunctionFactory(factory, alias, timeoutMs);
        }

        /**
         * Adds an {@link EvaluationFunctionStateFactory} to the chain of {@link EvaluationFunction}s.
         *
         * @param factory that creates new instances of a {@link EvaluationFunction}, when this step is on.
         * @param alias the alias for the results of the {@link EvaluationFunction}.
         * @return the {@link Builder}
         */
        public Builder addEvaluationFunctionFactory(EvaluationFunctionStateFactory factory, String alias){
            return addEvaluationFunctionFactory(factory, alias, DEFAULT_TIMEOUT_MS);
        }

        /**
         * Adds an {@link EvaluationFunctionStateFactory} to the chain of {@link EvaluationFunction}s.
         *
         * @param factory that creates new instances of a {@link EvaluationFunction}, when this step is on.
         * @param alias the alias for the results of the {@link EvaluationFunction}.
         * @param timeoutMs the chain step timeout
         * @return the {@link Builder}
         */
        public Builder addEvaluationFunctionFactory(EvaluationFunctionStateFactory factory, String alias, long timeoutMs) {
            if (this.stateConfigs == null) {
                this.stateConfigs = new ArrayList<>();
            }
            this.stateConfigs.add(new StateConfig(alias, factory, timeoutMs));
            return this;
        }
    
        /**
         * @param overallTimeoutMs a timeout value for the complete processing duration of the processing of all inner
         *                         {@link EvaluationFunction}s
         * @return self
         */
        public Builder withOverallTimeoutMs(long overallTimeoutMs) {
            this.overallTimeoutMs = overallTimeoutMs;
            return this;
        }
    
        /**
         * @param errorExtractor is a construct, that generates an error {@link Event} if a timeout should been raised
         * @return self
         */
        public Builder withErrorExtractor(StateErrorExtractor errorExtractor) {
            this.errorExtractor = errorExtractor;
            return this;
        }
    
        /**
         * @param stateCompleteExtractor is a construct, that evaluates all inner {@link EvaluationFunction}s results to
         *                               generate final {@link Event}s to be send out of the {@link ChainedEvaluationFunction}
         * @return self
         */
        public Builder withStateCompleteExtractor(ChainProcessingCompleteExtractor stateCompleteExtractor) {
            this.stateCompleteExtractor = stateCompleteExtractor;
            return this;
        }

        public ChainedEvaluationFunction build() {
            return new ChainedEvaluationFunction(
                    stateConfigs,
                overallTimeoutMs,
                errorExtractor,
                stateCompleteExtractor);
        }
    }
}
