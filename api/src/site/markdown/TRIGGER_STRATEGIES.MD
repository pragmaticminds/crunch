# TriggerStrategies
Library of static available [TriggerStrategy](TRIGGER_STRATEGY.html) implementations.

All of contained implementations require a [Supplier\<T\>](SUPPLIER.html) definition to extract the value of interest.

## Available strategies

- onTrue([Supplier\<T\>](SUPPLIER.html) supplier) : Fires when the supplied value is "true".

- onFalse([Supplier\<T\>](SUPPLIER.html) supplier) : Fires when the supplied value is "false".

- onBecomeTrue([Supplier\<T\>](SUPPLIER.html) supplier): Fires when the supplied value is "true" and was "false" before.

- onBecomeFalse([Supplier\<T\>](SUPPLIER.html) supplier): Fires when the supplied value is "false" and was "true" before.

- onChange([Supplier\<T\>](SUPPLIER.html) supplier): Fires when the supplied value is not the same as the one before.

- always(): Fires always. Does not need a [Supplier\<T\>](SUPPLIER.html).