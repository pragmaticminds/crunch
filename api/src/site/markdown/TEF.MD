#TriggeredEvaluationFunction
This is a implementation of the [EvaluationFunction](EVALUATION_FUNCTION.html)

## Challenge :

The situation where an implementation of the [EvlauationFunction](EVALUATION_FUNCTION.html) is done only to watch out for 
changes on a specific channel value is a common one. This leads to the requirement for a default structure that already has 
the logic needed to recognize changes on channel values of interest. 

## Soulution :

The TriggeredEvaluationFunction. It can watch the values on channels and trigger a result extraction, when the set 
condition is met. There for that implementation has the following members as a tool set:

 - [TriggerStrategy](TRIGGER_STRATEGY.html) : This is a class, that holds the rules on when to trigger a result creation 
 processing.
 - [EventExtractor](EVENT_EXTRACTOR.html) : This class creates resulting [Event](EVENT.html)s, when processing is triggered 
 by the [TriggerStrategy](TRIGGER_STRATEGY.html).
 - [EventFilter](EVENT_FILTER.html) : This class finally filters the resulting [Event](EVENT.html)s from the 
 [EventExtractor](EVENT_EXTRACTOR.html), so that only relevant [Event](EVENT.html)s are passed to the next stage in 
 processing.

## Code Example :

#### Instantiation:

```
TriggerEvaluationFunction function = new TriggerEvaluationFunction.Builder()
            // set the trigger strategy
            .withTriggerStrategy(TriggerStrategies.onBecomeTrue())
            // set the event extractor
            .withEventExtractor(Extractors.valuesExtractor("U", "I", "T"))
            // set the filter (optional)
            .withFilter(EventFilters.onValueChanged(Suppliers.doubleChannel("T")))
            .build();
```
In this case the [TriggerStrategy](TRIGGER_STRATEGY.html) \"onBecomeTrue" is used. This strategy triggers when a value that 
was false in the last message is now true.

As [EventExtractor](EVENT_EXTRACTOR.html) \"valuesExtractor(String\... channelName)" is used. This extracts the given 
channels by their name and puts the results into a new [Event](EVENT.html) object, which is returned after.

As [EventFilter](EVENT_FILTER.html) \"onValueChanged" is used. This Filter memorizes the value from the last call and 
passes [Event](EVENT.html)s only further, if the value has changed since last call.

As [Supplier](SUPPLIER.html) for the [EventFilter](EVENT_FILTER.html) the \"doubleChannel(String channelName)" is used. This 
Supplier extracts a channel value, defined by the name parameter.

#### Execution :

Since the TriggerEvaluationFunction is a implementation of the [EvaluationFunction](EVALUATION_FUNCTION.html), it can be 
executed as such. (see [EvaluationFunction](EVALUATION_FUNCTION.html))