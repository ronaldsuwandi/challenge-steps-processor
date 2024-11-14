package com.ronaldsuwandi.processor;

import com.ronaldsuwandi.model.UserStepsDaily;
import com.ronaldsuwandi.serde.NavigableMapSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.NavigableMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

public class ConsecutiveStepsProcessorTest {
    private ConsecutiveStepsProcessor processor;
    private MockProcessorContext<String, String> context;
    private KeyValueStore<String, NavigableMap<Long, String>> stateStore;

    @BeforeEach
    public void setUp() {
        processor = new ConsecutiveStepsProcessor();
        context = new MockProcessorContext();
        stateStore = Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(ConsecutiveStepsProcessor.STORE_NAME),
                        Serdes.String(),
                        new NavigableMapSerde())
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        stateStore.init(context.getStateStoreContext(), stateStore);
        context.addStateStore(stateStore);

        processor.init(context);
    }

    @Test
    public void testProcessUserWithLessThan7Days() {
        processor.process(new Record<>(
                new Windowed<>("user1", new TimeWindow(0, 1)),
                new UserStepsDaily("user1", 1000, Instant.now(), "daily_medium_stepper"),
                0
        ));
        processor.process(new Record<>(
                new Windowed<>("user1", new TimeWindow(1, 2)),
                new UserStepsDaily("user1", 1000, Instant.now(), "daily_medium_stepper"),
                1
        ));
        processor.process(new Record<>(
                new Windowed<>("user1", new TimeWindow(1, 2)),
                new UserStepsDaily("user1", 2000, Instant.now(), "daily_medium_stepper"),
                1
        ));
        KeyValueStore<String, NavigableMap<Long, String>> stateStore = context.getStateStore(ConsecutiveStepsProcessor.STORE_NAME);
        assertEquals(3, context.forwarded().size());
        assertEquals(2,  stateStore.get("user1").size()); // we should only update twice because timestamp 1 has 2 records (updated)
        assertEquals("", context.forwarded().getLast().record().value());
    }

    @Test
    public void testProcessUserWithSevenDaysMediumOrHigh() {
        // Add seven records with medium/high labels
        for (long i = 1; i <= 10; i++) {
            processor.process(new Record<>(
                    new Windowed<>("user1", new TimeWindow(i, i + 1)),
                    new UserStepsDaily("user1", 1000, Instant.now(), "daily_medium_stepper"),
                    i
            ));
        }
        KeyValueStore<String, NavigableMap<Long, String>> stateStore = context.getStateStore(ConsecutiveStepsProcessor.STORE_NAME);
        assertEquals(10, context.forwarded().size()); // processor forwards 10x
        assertEquals(7,  stateStore.get("user1").size()); // we should only care about the 7 days worth of data
        assertEquals("medium_or_high", context.forwarded().getLast().record().value());
    }

    @Test
    public void testProcessUserWithSevenGaps() {
        // Add seven records with medium/high labels
        for (long i = 1; i <= 6; i++) {
            processor.process(new Record<>(
                    new Windowed<>("user1", new TimeWindow(i, i + 1)),
                    new UserStepsDaily("user1", 1000, Instant.now(), "daily_medium_stepper"),
                    i
            ));
        }
        long gapStart=Duration.ofDays(4).toMillis();
        processor.process(new Record<>(
                new Windowed<>("user1", new TimeWindow(gapStart, gapStart+1)),
                new UserStepsDaily("user1", 1000, Instant.now(), "daily_medium_stepper"),
                gapStart
        ));
        KeyValueStore<String, NavigableMap<Long, String>> stateStore = context.getStateStore(ConsecutiveStepsProcessor.STORE_NAME);
        assertEquals(7, context.forwarded().size()); // processor forwards 10x
        assertEquals(7,  stateStore.get("user1").size());
        assertEquals("", context.forwarded().getLast().record().value()); // user has a gap, so we do
    }

    @Test
    public void testAllTimestampContinous() {
        Instant now = Instant.now();
        TreeSet<Long> timestamps = new TreeSet<>();

        timestamps.add(now.toEpochMilli());
        timestamps.add(now.plus(1, ChronoUnit.DAYS).toEpochMilli());
        timestamps.add(now.plus(2, ChronoUnit.DAYS).toEpochMilli());
        timestamps.add(now.plus(3, ChronoUnit.DAYS).toEpochMilli());
        timestamps.add(now.plus(4, ChronoUnit.DAYS).toEpochMilli());
        timestamps.add(now.plus(5, ChronoUnit.DAYS).toEpochMilli());

        assertTrue(ConsecutiveStepsProcessor.allTimestampsContinuous(timestamps, Duration.ofDays(1)));
    }

    @Test
    public void testAllTimestampContinousNotEnoughData() {
        Instant now = Instant.now();
        TreeSet<Long> timestamps = new TreeSet<>();

        timestamps.add(now.toEpochMilli());

        assertTrue(ConsecutiveStepsProcessor.allTimestampsContinuous(timestamps, Duration.ofDays(1)));
    }

    @Test
    public void testAllTimestampContinousMissingGap() {
        Instant now = Instant.now();
        TreeSet<Long> timestamps = new TreeSet<>();

        timestamps.add(now.toEpochMilli());
        timestamps.add(now.plus(1, ChronoUnit.DAYS).toEpochMilli());
        timestamps.add(now.plus(2, ChronoUnit.DAYS).toEpochMilli());
        timestamps.add(now.plus(5, ChronoUnit.DAYS).toEpochMilli());

        assertFalse(ConsecutiveStepsProcessor.allTimestampsContinuous(timestamps, Duration.ofDays(1)));
    }

}
