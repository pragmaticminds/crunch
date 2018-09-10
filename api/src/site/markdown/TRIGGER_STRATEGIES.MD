# TriggerStrategies
Library of static available [TriggerStrategy](TRIGGER_STRATEGY.html) implementations.

All of contained implementations require a [Supplier\<T\>](SUPPLIER.html) definition to extract the value of interest.

## Available strategies

- onTrue([Supplier\<Boolean\>](SUPPLIER.html) supplier) : Fires when the supplied value is "true".

- onFalse([Supplier\<Boolean\>](SUPPLIER.html) supplier) : Fires when the supplied value is "false".

- onBecomeTrue([Supplier\<Boolean\>](SUPPLIER.html) supplier): Fires when the supplied value is "true" and was "false" before.

- onBecomeFalse([Supplier\<Boolean\>](SUPPLIER.html) supplier): Fires when the supplied value is "false" and was "true" before.

- onChange([Supplier\<T\>](SUPPLIER.html) supplier): Fires when the supplied value is not the same as the one before.

- onNull([Supplier\<T\>](SUPPLIER.html)): Fires when the supplied channel is not available or set with a null value. 

- onNull([Supplier\<T\>](SUPPLIER.html)): Fires when the supplied channel is set and has an other value as null. 

- always(): Fires always. Does not need a [Supplier\<T\>](SUPPLIER.html).
