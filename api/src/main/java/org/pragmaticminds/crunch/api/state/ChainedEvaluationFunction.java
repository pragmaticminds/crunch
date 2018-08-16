package org.pragmaticminds.crunch.api.state;

import com.google.common.base.Preconditions;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.timer.ReferenceTimer;
import org.pragmaticminds.crunch.api.pipe.timer.Timer;
import org.pragmaticminds.crunch.api.trigger.Tuple2;
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
    
    private final ArrayList<Tuple2<EvaluationFunctionStateFactory, Long>> stateFactoriesAndTimeouts;
    private final Long                                                    overallTimeoutMs;
    
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
     * @param stateFactoriesAndTimeouts is a list of Tuple2 values, which contains a {@link EvaluationFunction} factory
     *                                  and a timeout for the chain step
     * @param overallTimeoutMs a timeout value for the complete processing duration of the processing of all inner
     *                         {@link EvaluationFunction}s
     * @param errorExtractor is a construct, that generates an error {@link Event} if a timeout should been raised
     * @param stateCompleteExtractor is a construct, that evaluates all inner {@link EvaluationFunction}s results to
     *                               generate final {@link Event}s to be send out of the {@link ChainedEvaluationFunction}
     */
    private ChainedEvaluationFunction(
        List<Tuple2<EvaluationFunctionStateFactory, Long>> stateFactoriesAndTimeouts,
        Long overallTimeoutMs,
        StateErrorExtractor errorExtractor,
        ChainProcessingCompleteExtractor stateCompleteExtractor
    ) {
        this.stateFactoriesAndTimeouts = new ArrayList<>(stateFactoriesAndTimeouts);
        this.overallTimeoutMs = overallTimeoutMs;
        this.errorExtractor = errorExtractor;
        this.stateCompleteExtractor = stateCompleteExtractor;
        
        this.currentStep = 0;
        this.currentStateEvaluationFunction = stateFactoriesAndTimeouts.get(currentStep).getF0().create();
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
        if(stateFactoriesAndTimeouts.get(currentStep).getOptionalF1().isPresent() && stateTimer == null){
            stateTimer = context.createNewTimer(this);
            resetStateTimeout(stateFactoriesAndTimeouts.get(currentStep).getF1());
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
            innerContext = new StateEvaluationContext(context.get()) {
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
        if(stateFactoriesAndTimeouts.get(currentStep).getOptionalF1().isPresent()
        && innerContext.getEvents().containsKey(TIMEOUT)) {
            errorExtractor.process(innerContext.getEvents(), null, context);
            resetStatemachine();
        // if last state completed
        }else if(currentStep == stateFactoriesAndTimeouts.size() - 1){
            stateCompleteExtractor.process(events, context);
            resetStatemachine();
        }else{
            currentStep++;
            currentStateEvaluationFunction = stateFactoriesAndTimeouts.get(currentStep).getF0().create();
            stateFactoriesAndTimeouts.get(currentStep)
                .getOptionalF1()
                .ifPresent(this::resetStateTimeout);
        }
    }
    
    /**
     * resets the inner structures in this instance, to begin processing from the start
     */
    private void resetStatemachine() {
        // restart the processing
        currentStep = 0;
        currentStateEvaluationFunction = stateFactoriesAndTimeouts.get(currentStep).getF0().create();
        
        // reset overall timeout and state timeout
        stateFactoriesAndTimeouts.get(currentStep).getOptionalF1()
            .ifPresent(this::resetStateTimeout);
        
        if(overallTimeoutMs!=null){
            resetOverallTimeout(overallTimeoutMs);
        }
        
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
        private List<Tuple2<EvaluationFunctionStateFactory, Long>> stateFactoriesAndTimeouts;
        private Long                                               overallTimeoutMs;
        private StateErrorExtractor                                errorExtractor;
        private ChainProcessingCompleteExtractor                   stateCompleteExtractor;
        
        private Builder() {}
    
        /**
         * @param stateFactoriesAndTimeouts is a list of Tuple2 values, which contains a {@link EvaluationFunction} factory
         *                                  and a timeout for the chain step
         * @return self
         */
        public Builder withStateFactoriesAndTimeouts(List<Tuple2<EvaluationFunctionStateFactory, Long>> stateFactoriesAndTimeouts) {
            this.stateFactoriesAndTimeouts = stateFactoriesAndTimeouts;
            return this;
        }
    
        /**
         * @param overallTimeoutMs a timeout value for the complete processing duration of the processing of all inner
         *                         {@link EvaluationFunction}s
         * @return self
         */
        public Builder withOverallTimeoutMs(Long overallTimeoutMs) {
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
    
        public Builder but() {
            checkValues();
            return builder()
                .withStateFactoriesAndTimeouts(stateFactoriesAndTimeouts)
                .withOverallTimeoutMs(overallTimeoutMs)
                .withErrorExtractor(errorExtractor)
                .withStateCompleteExtractor(stateCompleteExtractor);
        }
    
        /**
         * check if all necessary values are set
         */
        private void checkValues() {
            Preconditions.checkNotNull(stateFactoriesAndTimeouts);
            Preconditions.checkNotNull(errorExtractor);
            Preconditions.checkNotNull(stateCompleteExtractor);
        }
    
        public ChainedEvaluationFunction build() {
            return new ChainedEvaluationFunction(stateFactoriesAndTimeouts,
                                                 overallTimeoutMs,
                                                 errorExtractor,
                                                 stateCompleteExtractor);
        }
    }
}
