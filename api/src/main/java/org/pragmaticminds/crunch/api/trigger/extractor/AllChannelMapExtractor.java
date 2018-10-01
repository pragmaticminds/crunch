package org.pragmaticminds.crunch.api.trigger.extractor;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Extracts all channel values into a {@link Map} from the {@link MRecord} inside the {@link EvaluationContext}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.09.2018
 */
class AllChannelMapExtractor implements MapExtractor {
    
    /**
     * Extracts all channels from the {@link MRecord}.
     * This method extracts a map of {@link Value}s from a {@link EvaluationContext}, in particular from it's
     * {@link MRecord}.
     *
     * @param context the current {@link EvaluationContext} that holds the current {@link MRecord}.
     * @return a {@link Map} of keyed extracted values from the {@link EvaluationContext}s {@link MRecord}.
     */
    @Override
    public Map<String, Value> extract(EvaluationContext context) {
        return context.get().getChannels().stream().collect(Collectors.toMap(
            channel -> channel,
            channel -> context.get().getValue(channel)
        ));
    }
}
