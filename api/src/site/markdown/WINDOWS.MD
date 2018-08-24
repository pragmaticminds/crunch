# Wiondows
This is a library collection class of implementations of [RecordWindow](RECORD_WINDOW.html). All contents of the library 
are statically available.

## Availabel implementations

- bitActive([Supplier\<T\>](SUPPLIER.html) supplier): window is open when the supplied value is "true".

- bitNotActive([Supplier\<T\>](SUPPLIER.html) supplier): window is open when the supplied value is "false".

- valueEquals([Supplier\<T\>](SUPPLIER.html) supplier, T expected): window is open when the supplied value is equal to the 
expected value.

- valueNotEquals([Supplier\<T\>](SUPPLIER.html) supplier, T notExpected): window is open when the supplied value is not 
equal to the notExpected value.