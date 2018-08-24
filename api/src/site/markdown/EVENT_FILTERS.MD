# EventFilters
This class is a library with static available typical implementations of the [EventFilter](EVENT_FILTER.html) interface.

## Available implementations:

- onValueChanged([Supplier\<T\>](SUPPLIER.html) supplier) : Watches on changes on an MRecord. Only when the supplied value
has changes since the last execution of this method, [Event](EVENT.html)s are not filtered out.