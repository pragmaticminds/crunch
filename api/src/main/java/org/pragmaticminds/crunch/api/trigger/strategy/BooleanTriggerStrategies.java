package org.pragmaticminds.crunch.api.trigger.strategy;

/**
 * This {@link TriggerStrategy} collection reacts on a {@link Boolean} decision base. By calling the right builder
 * method the strategy with the required behavior is created.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.07.2018
 */
public class BooleanTriggerStrategies {

    private BooleanTriggerStrategies() { /* do nothing */}

    /**
     * Triggers on positive incoming decisionBase values
     *
     * @return true if decisionBase is also true otherwise false
     */
    public static TriggerStrategy<Boolean> onTrue() {
        return decisionBase -> decisionBase;
    }

    /**
     * Triggers on negative incoming decisionBase values
     *
     * @return true if decisionBase is false otherwise false
     */
    public static TriggerStrategy<Boolean> onFalse() {
        return decisionBase -> !decisionBase;
    }

    /**
     * comparison value has changed compared to the last call
     *
     * @return true if last oncoming value was different to the current
     */
    public static TriggerStrategy<Boolean> onChange() {
        return new TriggerStrategy<Boolean>() {
            private Boolean lastDecisionBase = null;

            /** @inheritDoc */
            @Override
            public boolean isToBeTriggered(Boolean decisionBase) {
                boolean result = false;
                if (lastDecisionBase != null) {
                    result = decisionBase != lastDecisionBase;
                }
                lastDecisionBase = decisionBase;
                return result;
            }
        };
    }

    /**
     * comparison value has changed and is true compared to the last call
     *
     * @return true if last oncoming value was different to the current and the current is positive
     */
    public static TriggerStrategy<Boolean> onBecomeTrue() {
        return new TriggerStrategy<Boolean>() {
            private Boolean lastDecisionBase = null;

            /** @inheritDoc */
            @Override
            public boolean isToBeTriggered(Boolean decisionBase) {
                boolean result = false;
                if (lastDecisionBase != null) {
                    result = !lastDecisionBase && decisionBase != lastDecisionBase;
                }
                lastDecisionBase = decisionBase;
                return result;
            }
        };
    }

    /**
     * comparison value has changed and is false compared to the last call
     *
     * @return true if last oncoming value was different to the current and the current is negative
     */
    public static TriggerStrategy<Boolean> onBecomeFalse() {
        return new TriggerStrategy<Boolean>() {
            private Boolean lastDecisionBase = null;

            /** @inheritDoc */
            @Override
            public boolean isToBeTriggered(Boolean decisionBase) {
                boolean result = false;
                if (lastDecisionBase != null) {
                    result = lastDecisionBase && decisionBase != lastDecisionBase;
                }
                lastDecisionBase = decisionBase;
                return result;
            }
        };
    }

    /**
     * always triggers on incoming values
     *
     * @return always true
     */
    public static TriggerStrategy<Boolean> always() {
        return decisionBase -> true;
    }
}

