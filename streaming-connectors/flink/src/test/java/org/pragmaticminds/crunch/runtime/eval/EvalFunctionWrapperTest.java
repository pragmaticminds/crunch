package org.pragmaticminds.crunch.runtime.eval;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.AnnotatedEvalFunctionWrapper;
import org.pragmaticminds.crunch.api.EvalFunctionCall;
import org.pragmaticminds.crunch.api.evaluations.annotated.RegexFind2;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Unit Tests for EvalFunctionWrapper
 *
 * @author julian
 * Created by julian on 14.12.17
 */
public class EvalFunctionWrapperTest {

    private EvalFunctionCall call;
    private TypedValues typedValues;

    @Before
    public void setUp() throws Exception {
        call = new EvalFunctionCall(
                new AnnotatedEvalFunctionWrapper<>(RegexFind2.class),
                Collections.singletonMap("regex", Value.of("nothing")),
                Collections.singletonMap("value", "DB13_StringChannel")
        );
        Map<String, Value> valueMap = new HashMap<>();
        valueMap.put("DB13_StringChannel", Value.of("a_string"));
        valueMap.put("DB13_LongChannel", Value.of(100L));
        typedValues = TypedValues.builder()
                .source("no_source")
                .timestamp(100L)
                .values(valueMap)
                .build();
    }

    @Test
    public void createTypedChannelMap_stringToString_works() {
        EvalFunctionWrapper evalFunctionWrapper = new EvalFunctionWrapper(call);
        Map<String, Value> typedChannelMap = evalFunctionWrapper.createTypedChannelMap(typedValues);

        assertEquals(1, typedChannelMap.size());
        assertEquals("a_string", typedChannelMap.get("DB13_StringChannel").getAsString());
    }

    @Test
    public void createTypedChannelMap_longToString_works() throws Exception {
        EvalFunctionCall call = new EvalFunctionCall(
                new AnnotatedEvalFunctionWrapper<>(RegexFind2.class),
                Collections.singletonMap("regex", Value.of("nothing")),
                Collections.singletonMap("value", "DB13_LongChannel"));
        EvalFunctionWrapper evalFunctionWrapper = new EvalFunctionWrapper(call);
        Map<String, Value> typedChannelMap = evalFunctionWrapper.createTypedChannelMap(typedValues);

        assertEquals(1, typedChannelMap.size());
        assertEquals("100", typedChannelMap.get("DB13_LongChannel").getAsString());
    }

}
