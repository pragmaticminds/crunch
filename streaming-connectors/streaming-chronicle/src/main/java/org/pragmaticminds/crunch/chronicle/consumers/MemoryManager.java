package org.pragmaticminds.crunch.chronicle.consumers;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link ConsumerManager} which uses an in memory map.
 * Useful for Testing.
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public class MemoryManager implements ConsumerManager {

    private Map<String, Long> offsetMap = new HashMap<>();

    @Override
    public long getOffset(String consumer) {
        if (offsetMap.containsKey(consumer)) {
            return offsetMap.get(consumer);
        } else {
            return -1;
        }
    }

    @Override
    public void acknowledgeOffset(String consumer, long offset) {
        offsetMap.put(consumer, offset);
    }

    @Override
    public void close() {
        // do nothing here
    }
}
