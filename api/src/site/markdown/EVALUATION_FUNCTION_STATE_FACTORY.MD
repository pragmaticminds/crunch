# EvaluationFunctionStateFactory (Interface)

This is a default [EvaluationFunction](EVALUATION_FUNCTION.html) factory. It has a create method, on which it creates a 
new or cloned instance of the [EvaluationFunction](EVALUATION_FUNCTION.html) of the factory.

## Available implementations :

- __[CloneStateEvaluationFunctionFactory](CLONE_STATE_EVALUATION_FUNCTION_FACTORY.html)__ a factory that takes an instance
of a [EvaluationFunction](EVALUATION_FUNCTION.html) as prototype and clones it every time the "create" method is called.