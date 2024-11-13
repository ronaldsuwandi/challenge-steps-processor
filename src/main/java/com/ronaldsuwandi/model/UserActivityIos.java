package com.ronaldsuwandi.model;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record UserActivityIos(
        @JsonProperty("UserFirstName")
        String firstName,
        @JsonProperty("UserLastName")
        String lastName,
        @JsonProperty("UserAddress")
        String address,
        @JsonProperty("UserId")
        String id,
        @JsonProperty("UserGender")
        String gender,
        @JsonProperty("UserHeight")
        double height,
        @JsonProperty("UserWeight")
        double weight,
        @JsonProperty("PhysicalActivity")
        PhysicalActivity activity,
        @JsonProperty("Geo_lat")
        Double latitude,
        @JsonProperty("Geo_lon")
        Double longitude,
        String pincode,
        @JsonProperty("event_timestamp")
        Instant timestamp
) {
    public record PhysicalActivity(
            @JsonProperty("Dance_minutes")
            int danceMinutes,
            @JsonProperty("Yoga_minutes")
            int yogaMinutes,
            @JsonProperty("Sleep_hours")
            int sleepHours,
            @JsonProperty("walking_steps")
            int walkingSteps
    ) {
    }
}
