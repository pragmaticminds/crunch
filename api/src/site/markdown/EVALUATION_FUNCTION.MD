# EvaluationFunction (Interface)

This interface represents the main processing entity in the Crunch environment. It is the interface of all 
EvaluationFunction implementations. It can be implemented as a lambda, because it has a "@FunctionalInterface" 
annotation. It has a eval function for processing an [EvaluationContext](EVALUATION_CONTEXT.html), which has an incoming 
value and handles outgoing results for the EvaluationFunction.


## Common Use Case Implementations

[TEF - TriggeredEvaluationFunction](TEF.html)

[WEF - WindowedEvaluationFunction](WEF.html)