# TriggerStrategy (Interface)

## Use Case
Implementations of this interface are used to determine if a condition for triggering the extraction process in a 
[TriggerEvaluationFunction](TEF.html) is to be executed.

## Lambda example implementations

#### Simple example on a Boolean channel
```
TriggerStrategy strategy = (TriggerStrategy)(values) -> values.getBoolean("flag");
```

In this case the trigger is fired, when an MRecord has the channel "flag" as boolean and its value is "true".

#### Example on a Double channel

```
TriggerStrategy strategy = (TriggerStrategy)(values) -> values.getDouble("U") > 320.0;
```

In this case the trigger is fired, when an MRecord has the channel "U" as Double and its value is greater than 320.

## Available implementations

- [TriggerStrategies](TRIGGER_STRATEGIES.html) hold a collection of typical implementations.