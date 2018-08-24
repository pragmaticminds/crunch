package org.pragmaticminds.crunch.chronicle;

import org.junit.Test;
import org.mockito.Mockito;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author julian
 * Created by julian on 21.08.18
 */
public class ChronicleSourceTest {

    @Test
    public void create_noInitialize() {
        ChronicleSource.ChronicleConsumerFactory factory = Mockito.mock(ChronicleSource.ChronicleConsumerFactory.class);
        ChronicleSource source = new ChronicleSource(factory);

        source.close();

        // Assert no instantiation
        verify(factory, never()).create();
    }

    @Test
    public void initialize() {
        ChronicleConsumer consumer = Mockito.mock(ChronicleConsumer.class);
        ChronicleSource source = new ChronicleSource(() -> consumer);

        source.init();

        assertTrue(source.hasRemaining());

        source.get();
        source.close();

        // Assert poll on source
        verify(consumer, times(1)).poll();
    }
}