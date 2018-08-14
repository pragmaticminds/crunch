package org.pragmaticminds.crunch.api.trigger.strategy;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.07.2018
 */
public class BooleanTriggerStrategiesTest {

    @Test
    public void isToBeTriggeredOnTruePositive() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.onTrue();
        boolean result = strategy.isToBeTriggered(true);
        Assert.assertTrue(result);
    }

    @Test
    public void isToBeTriggeredOnTrueNegative() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.onTrue();
        boolean result = strategy.isToBeTriggered(false);
        Assert.assertFalse(result);
    }

    @Test
    public void isToBeTriggeredOnFalsePositive() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.onFalse();
        boolean result = strategy.isToBeTriggered(false);
        Assert.assertTrue(result);
    }

    @Test
    public void isToBeTriggeredOnFalseNegative() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.onFalse();
        boolean result = strategy.isToBeTriggered(true);
        Assert.assertFalse(result);
    }

    @Test
    public void isToBeTriggeredOnChangePositive() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.onChange();

        strategy.isToBeTriggered(false);
        boolean result = strategy.isToBeTriggered(true);
        Assert.assertTrue(result);

        strategy.isToBeTriggered(true);
        boolean result2 = strategy.isToBeTriggered(false);
        Assert.assertTrue(result2);
    }

    @Test
    public void isToBeTriggeredOnChangeNegative() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.onChange();

        strategy.isToBeTriggered(false);
        boolean result = strategy.isToBeTriggered(false);
        Assert.assertFalse(result);

        strategy.isToBeTriggered(true);
        boolean result2 = strategy.isToBeTriggered(true);
        Assert.assertFalse(result2);
    }

    @Test
    public void isToBeTriggeredOnBecomeTruePositive() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.onBecomeTrue();
        strategy.isToBeTriggered(false);
        boolean result = strategy.isToBeTriggered(true);
        Assert.assertTrue(result);
    }

    @Test
    public void isToBeTriggeredOnBecomeTrueNegative() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.onBecomeTrue();
        strategy.isToBeTriggered(true);
        boolean result = strategy.isToBeTriggered(false);
        Assert.assertFalse(result);
    }

    @Test
    public void isToBeTriggeredOnBecomeFalsePositive() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.onBecomeFalse();
        strategy.isToBeTriggered(true);
        boolean result = strategy.isToBeTriggered(false);
        Assert.assertTrue(result);
    }

    @Test
    public void isToBeTriggeredOnBecomeFalseNegative() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.onBecomeFalse();
        strategy.isToBeTriggered(false);
        boolean result = strategy.isToBeTriggered(true);
        Assert.assertFalse(result);
    }

    @Test
    public void isToBeTriggeredAllways() {
        TriggerStrategy<Boolean> strategy = BooleanTriggerStrategies.always();
        boolean result = strategy.isToBeTriggered(false);
        Assert.assertTrue(result);
    }
}