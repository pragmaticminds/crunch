package org.pragmaticminds.crunch.api;

import org.junit.Test;
import org.pragmaticminds.crunch.api.evaluations.annotated.RegexFind2;
import org.pragmaticminds.crunch.api.records.DataType;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link EvalFunctionCall}
 *
 * @author julian
 * Created by julian on 05.11.17
 */
public class EvalFunctionCallTest {

    @Test
    public void shouldGetNecessaryChannelsAndTypes_fromEvalFunctionCall() throws Exception {
        EvalFunctionCall call = new EvalFunctionCall(new AnnotatedEvalFunctionWrapper<>(RegexFind2.class),
                Collections.singletonMap("regex", Value.of("nothing")),
                Collections.singletonMap("value", "DB14_Channel123"));

        Map<String, DataType> channelsAndTypes = call.getChannelsAndTypes();

        assertEquals(1, channelsAndTypes.size());
        assertEquals(DataType.STRING, channelsAndTypes.get("DB14_Channel123"));
    }
}