# Supplier\<T\> (Interface)

## Use Case
Implementation of this interface are used to extract a channel value of interest from a MRecord.

## Lambda example implementations
#### Simple example on a Boolean channel

```
Supplier<Boolean> supplier = (Supplier<Boolean>)(values) -> values.getBoolean("flag");
```

In this case the supplier implementation is extracting the Boolean channel value with the name "flag".

#### Example on a Double channel

```
Supplier<Double> supplier = (Supplier<Double>)(values) -> values.getDouble("U");
```

In this case the supplier implementation is extracting the Double channel value with the name "U".

#### Example on two values at once

```
Supplier<Tuple2<Double, Boolean>> supplier = (Supplier<Tuple2<Double, Boolean>>) 
    (values) -> new Tuple2(values.getBoolean("flag"), values.getDouble("U"));
```

In this case the supplier implementation is extraction the both values from the previous examples at once.
It also could have returned a List or similar.

## Available implementations

- [Suppliers](SUPPLIERS.html) is a collection of typicle use case implementations.