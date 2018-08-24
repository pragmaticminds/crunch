# RecordWindow (Interface)
This interface has a "inWindow(MRecord record)" method. The implementations of this must determine if the window is
open or closed. While a window is open the [WindowEvaluationFunction](WEF.html) aggregates incoming records, where it
extracts the channel values of interest of the MRecords.

## Simple implementation example

```
RecordWindow window = (record) -> record.getBoolean("flag");
```

In this example the window is always open when the Boolean channel "flag" has the value "true".

```
RecordWindow window = (record) -> record.getString("id").equals("Mixer17");
``` 

In this example the window is always open when the String channel "id" has the value "Mixer17".

##Implementations for typical use cases

- [Windows](WINDOWS.html) a library of static implementations of this interface.