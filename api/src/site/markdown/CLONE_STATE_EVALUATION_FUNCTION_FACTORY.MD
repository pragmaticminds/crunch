# CloneStateEvaluationFunctionFactory

This is a simple cloning implementation of the [EvaluationFunctionStateFactory](EVALUATION_FUNCTION_STATE_FACTORY.html) 
interface. In its constructor this class takes an original/prototype instance of a 
[EvaluationFunction](EVALUATION_FUNCTION.html) and clones it every time the method "create" is called.

## Code example

```
// create a simple EvaluationFunction
EvaluationFunction evaluationFunctionOriginal = (context) -> {
    if(context.get().getDouble("U") > 23.0){
        context.collect(
            context.getEventBuilder()
                .withEvent("U_IS_HIGHER_23_VOLTAGE")
                .withSource(context.get().getSource())
                .withTimestamp(context.get().getTimestamp())
                .withParameter("system time", System.getCurrentTimestamp())
                .build();
        );
    }
};

...

// create the factory
CloneStateEvaluationFunctionFactory factory = CloneStateEvaluationFunctionFactory.builder()
    .withPrototype(evaluationFunctionOriginal)
    .build();

...

EvaluationFunction currentInstance = Evalufactory.create();

```
In this example first an original instance of a [EvaluationFunction](EVALUATION_FUNCTION.html) is created.
This instance is passed to the Builder of the CloneStateEvaluationFunctionFactory and build is called, so that there is
a factory instance.

To create untouched instances of the [EvaluationFunction](EVALUATION_FUNCTION.html) the method "create" is called and the
new instance of the [EvaluationFunction](EVALUATION_FUNCTION.html) can be used for processing.