package com.ronaldsuwandi.processor;

import com.ronaldsuwandi.model.UserStepsDaily;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsecutiveStepsProcessor implements Processor<Windowed<String>, UserStepsDaily, String, String> {
    private KeyValueStore<String, NavigableMap<Long, String>> stateStore;
    private ProcessorContext<String, String> context;
    private static Logger logger = LoggerFactory.getLogger(ConsecutiveStepsProcessor.class);
    public final static String STORE_NAME = "user-steps-7-days-rolling-store";

    @Override
    public void init(ProcessorContext<String, String> context) {
        // Initialize the state store to keep 7-day history of labels
        this.stateStore = context.getStateStore(STORE_NAME);
        this.context = context;
    }

    @Override
    public void process(Record<Windowed<String>, UserStepsDaily> record) {
        String userId = record.key().key();
        Long timeWindowStart = record.key().window().start();
        String label = record.value().label();

        NavigableMap<Long, String> labelHistory = stateStore.get(userId);
        if (labelHistory == null) {
            labelHistory = new TreeMap<>();
        }

        logger.trace("> BEFORE label history size = {} => {}", labelHistory.size(), labelHistory.toString());
        if (labelHistory.containsKey(timeWindowStart)) {
            labelHistory.put(timeWindowStart,label);
        } else {
            // check for size, we keep track of only 7 days
            if (labelHistory.size() >= 7) {
                labelHistory.pollFirstEntry();
            }
            labelHistory.put(timeWindowStart, label);
        }

        stateStore.put(userId, labelHistory); // have to store this again

        long timestamp = Instant.now().toEpochMilli();
        logger.trace("> AFTER label history size = {} => {}", labelHistory.size(), labelHistory.toString());

        if (labelHistory.size() < 7) {
            // user doesn't have 7 days worth of history, skip
            context.forward(new Record<>(userId, "", timestamp));
        } else {
            boolean sevenDaysMediumOrHigh = labelHistory.values().stream().allMatch(l -> l.equals("daily_medium_stepper") || l.equals("daily_high_stepper"));
            logger.trace("> all seven days medium/high? {}", sevenDaysMediumOrHigh);
            if (sevenDaysMediumOrHigh) {
                context.forward(new Record<>(userId, "medium_or_high", timestamp));
            } else {
                context.forward(new Record<>(userId, "low", timestamp));
            }
        }
    }
}
