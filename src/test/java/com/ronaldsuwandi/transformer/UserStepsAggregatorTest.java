package com.ronaldsuwandi.transformer;

import com.ronaldsuwandi.model.UserActivityNormalized;
import com.ronaldsuwandi.model.UserStepsDaily;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UserStepsAggregatorTest {
    private UserStepsAggregator aggregator;

    @BeforeEach
    public void setUp() {
        aggregator = new UserStepsAggregator();
    }

    @ParameterizedTest
    @MethodSource("provideAggregations")
    public void testAggregation(int initialSteps, int additionalSteps, int expectedSteps, String expectedLabel) {
        UserStepsDaily initialAggregate = new UserStepsDaily("user1", initialSteps, Instant.now(), "");
        UserActivityNormalized activity = new UserActivityNormalized("user1", "firstname", "lastname", additionalSteps, null, null, null, Instant.now());

        UserStepsDaily result = aggregator.apply("user1", activity, initialAggregate);

        assertEquals(expectedSteps, result.steps());
        assertEquals(expectedLabel, result.label());

    }

    private static Stream<Arguments> provideAggregations() {
        return Stream.of(
                Arguments.of(500, 400, 900, "daily_low_stepper"),
                Arguments.of(800, 300, 1100, "daily_medium_stepper"),
                Arguments.of(2000, 2500, 4500, "daily_medium_stepper"),
                Arguments.of(4000, 1200, 5200, "daily_high_stepper"),
                Arguments.of(5000, 3000, 8000, "daily_high_stepper")
        );
    }

    @Test
    public void testAggregationResetsDay() {
        Instant timestamp = Instant.now();
        UserStepsDaily initialAggregate = new UserStepsDaily("user1", 1500, timestamp, "daily_medium_stepper");
        UserActivityNormalized activity = new UserActivityNormalized("user1", "firstname", "lastname", 400, null, null, null, timestamp.plus(1, ChronoUnit.SECONDS));

        UserStepsDaily result = aggregator.apply("user1", activity, initialAggregate);

        assertEquals(timestamp.truncatedTo(ChronoUnit.DAYS), result.timestamp()); // Ensure day is truncated correctly
    }
}
