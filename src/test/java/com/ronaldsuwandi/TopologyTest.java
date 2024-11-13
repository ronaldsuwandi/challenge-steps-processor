package com.ronaldsuwandi;

import com.ronaldsuwandi.model.UserActivityNormalized;
import com.ronaldsuwandi.model.UserNotification;
import com.ronaldsuwandi.serde.JSONSerdeUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TopologyTest {
    TopologyTestDriver driver;
    TestInputTopic<String, UserActivityNormalized> inputTopic;
    TestOutputTopic<String, UserNotification> outputTopic;

    @BeforeEach
    public void setup() {
        Topology topology = new TopologyBuilder().build();

        Properties props = new Properties();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0); // disable caching for test

        driver = new TopologyTestDriver(topology, props);

        inputTopic = driver.createInputTopic(
                TopologyBuilder.TOPIC_USER_ACTIVITY_NORMALIZED,
                new StringSerializer(),
                JSONSerdeUtil.getSerde(UserActivityNormalized.class).serializer());

        outputTopic = driver.createOutputTopic(
                TopologyBuilder.TOPIC_USER_NOTIFICATIONS,
                new StringDeserializer(),
                JSONSerdeUtil.getSerde(UserNotification.class).deserializer());

    }

    @AfterEach
    public void teardown() {
        driver.close();
    }

    @Test
    public void testTopologySimple() {
        // simple example where user is always medium/high stepper for a week
        Instant timestamp = Instant.now();

        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(1, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 6000, null, null, null, timestamp.plus(2, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(3, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 9000, null, null, null, timestamp.plus(4, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2500, null, null, null, timestamp.plus(5, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(6, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(7, ChronoUnit.DAYS)));

        List<TestRecord<String, UserNotification>> result = outputTopic.readRecordsToList();
        assertEquals(1, result.size());
        assertEquals("user1", result.get(0).key());
    }

    @Test
    public void testTopologyNotificationSuppressed() {
        // on day 7, user1 has too many incoming data, we should suppress notification generation only to once so user is not bombarded
        Instant timestamp = Instant.now();

        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(1, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 6000, null, null, null, timestamp.plus(2, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(3, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 9000, null, null, null, timestamp.plus(4, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2500, null, null, null, timestamp.plus(5, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(6, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(7, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 5000, null, null, null, timestamp.plus(7, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(7, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 6000, null, null, null, timestamp.plus(7, ChronoUnit.DAYS).plusSeconds(2)));

        List<TestRecord<String, UserNotification>> result = outputTopic.readRecordsToList();
        assertEquals(1, result.size());
        assertEquals("user1", result.get(0).key());
    }

    @Test
    public void testTopologyNotificationSendAfterBecomingMediumStepper() {
        // initially on day 7 user is on low daily step count, so no notification sent
        Instant timestamp = Instant.now();
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(1, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 6000, null, null, null, timestamp.plus(2, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(3, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 9000, null, null, null, timestamp.plus(4, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2500, null, null, null, timestamp.plus(5, ChronoUnit.DAYS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 100, null, null, null, timestamp.plus(6, ChronoUnit.DAYS)));

        // should be empty
        List<TestRecord<String, UserNotification>> result = outputTopic.readRecordsToList();
        assertEquals(0, result.size());

        // later on user become medium stepper
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(6, ChronoUnit.DAYS).plus(1, ChronoUnit.HOURS)));
        inputTopic.pipeInput("user1", new UserActivityNormalized("user1", "first", "last", 2000, null, null, null, timestamp.plus(6, ChronoUnit.DAYS).plus(1, ChronoUnit.HOURS)));

        // next time we should receive additional 1 notification
        result = outputTopic.readRecordsToList();
        assertEquals(1, result.size());
        assertEquals("user1", result.get(0).key());
    }
}
