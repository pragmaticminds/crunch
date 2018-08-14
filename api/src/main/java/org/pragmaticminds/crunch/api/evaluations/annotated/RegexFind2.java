package org.pragmaticminds.crunch.api.evaluations.annotated;

import org.pragmaticminds.crunch.api.AnnotatedEvalFunction;
import org.pragmaticminds.crunch.api.annotations.*;
import org.pragmaticminds.crunch.api.events.EventHandler;
import org.pragmaticminds.crunch.api.holder.Holder;
import org.pragmaticminds.crunch.api.mql.DataType;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author kerstin
 * @author Erwin Wagasow
 * -> refactoring 25.10.2017
 * Created by kerstin on 20.06.17.
 */
@EvaluationFunction(evaluationName = "REGEX_FIND2", dataType = DataType.STRING, description = "Uses regex <b>find</b> on a string channel.")
public class RegexFind2 implements AnnotatedEvalFunction<String>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(RegexFind2.class);

    @ResultTypes(resultTypes = @ResultType(name = "match", dataType = DataType.STRING))
    private transient EventHandler eventHandler;

    @TimeValue
    private Holder<Long> time;

    @ChannelValue(name = "value", dataType = DataType.STRING)
    private Holder<String> s;

    @ParameterValue(name = "regex", dataType = DataType.STRING)
    private Holder<String> regexValue;

    private Pattern pattern;

    private long count = 0;
    private long found = 0;

    @Override
    public void setup() {
        if (logger.isDebugEnabled()) {
            logger.debug("Compiling patternValue");
        }
        pattern = Pattern.compile(regexValue.get().replaceAll("\\\\\\\\", "\\\\"));
    }


    @Override
    public String eval() {
        String str = s.get().replaceAll("[^\\u0020-\\u007F]", " ");
        Matcher m = pattern.matcher(str);
        count++;
        if (m.find()) {
            String substring = str.substring(0, Math.min(254, s.get().length()));
            eventHandler.fire(eventHandler.getBuilder()
                    .withTimestamp(Instant.now().toEpochMilli())
                    .withSource("none")
                    .withEvent("type")
                    .withParameter("find", Value.of(substring))
                    .build());
            found++;
            return substring;
        }
        return null;
    }

    @Override
    public void finish() {
        logger.info("Finished Regex Find[avg:{}; found:{}]", count, found);
    }
}
