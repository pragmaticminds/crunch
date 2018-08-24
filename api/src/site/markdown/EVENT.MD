# Event
The main result data object for the processing in an [EvaluationFunction](EVALUATION_FUNCTION.html) implementation.

It should only be created by the in a [EvaluationContext](EVALUATION_CONTEXT.html) available Builder. So that default 
values can already be set from the context. (not jet implemented that way, still this rule is to be followed!!!)

## Instantiation

A typical instatiation situation is when a [EvaluationContext](EVALUATION_CONTEXT.html) is in the scope.

```
Event event = context.getEventBuilder()
    // set the name
    .withEvent("nameOfTheEvent")
    // set the source
    .withSource(mRecord.getSource())
    // set the timestamp    
    .withTimestamp(mRecord.getTimestamp())
    // set the parameters
    .withParameter("U", Value.of(mRecord.get("U"))
    .withParameter("I", Value.of(mRecord.get("I"))
    .withParameter("T", Value.of(mRecord.get("T"))
    // let the Builder biuld the Event
    .build()
```