# Suppliers
Library of static available [Supplier\<T\>](SUPPLIER.html) implementations, that cover the typical use cases.

## Available Suppliers

#### ChannelExtractors

- booleanChannel(String name) : extracts Boolean channel values from the MRecord

- doubleChannel(String name) : extracts Double channel values from the MRecord

- longChannel(String name) : extracts Long channel values from the MRecord

- dateChannel(String name) : extracts Date channel values from the MRecord

- stringChannel(String name) : extracts String channel values from the MRecord

#### BooleanOperators

- and([Supplier\<Boolean\>](SUPPLIER.html) s1, [Supplier\<Boolean\>](SUPPLIER.html) s2) : logical and operation on both supplied values

- or([Supplier\<Boolean\>](SUPPLIER.html) s1, [Supplier\<Boolean\>](SUPPLIER.html) s2) : logical or operation on both supplied values 

- not([Supplier\<Boolean\>](SUPPLIER.html) supplier) : logical inverse of the supplied value

#### StringOperators

- equal(String expected, [Supplier\<String\>](SUPPLIER.html) supplier) : compares the supplied value with the expected String value. Returns a [Supplier\<Boolean\>](SUPPLIER.html).

- equal([Supplier\<String\>](SUPPLIER.html) s1, [Supplier\<String\>](SUPPLIER.html) s2) : compares both supplied values. Returns a [Supplier\<Boolean\>](SUPPLIER.html).

- match(String regex, [Supplier\<String\>](SUPPLIER.html) supplier) :  compares supplied values against a regex value for matching. Returns a [Supplier\<Boolean\>](SUPPLIER.html).

- contains(String string, [Supplier\<String\>](SUPPLIER.html) supplier) : Checks if the String value of the supplier contains the given string value. Returns a [Supplier\<Boolean\>](SUPPLIER.html).

- length([Supplier\<String\>](SUPPLIER.html) supplier) : determines the length of the supplied String value. Returns a [Supplier\<Long\>](SUPPLIER.html).

#### Comparators

- equals(T value, [Supplier\<T\>](SUPPLIER.html)) : checks whether value and supplied value are equal.

- equals([Supplier\<T\>](SUPPLIER.html) s1, [Supplier\<T\>](SUPPLIER.html) s2) : checks the two supplied values for equality.

- compare([Supplier\<T\>](SUPPLIER.html) s1, [Supplier\<T\>](SUPPLIER.html) s2) : determines a difference value for the 
two supplied values. If the values are equal the result is 0, if value of s1 is bigger, than the resulting number is >1,
if value of s2 is bigger, than the resulting value is negative.

- compare(T value, [Supplier\<T\>](SUPPLIER.html) supplier) : determines a difference value for the 
value and the supplied value. If the values are equal the result is 0, if value of s1 is bigger, than the resulting number is >1,
if value of s2 is bigger, than the resulting value is negative.