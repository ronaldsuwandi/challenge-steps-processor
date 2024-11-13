package com.ronaldsuwandi.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class JSONSerdeUtil {
    private static final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();

    static {
        // to enable JDK 8 date support
        objectMapper.registerModule(new JavaTimeModule());
        // ensure date is serialized using iso format
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public static <T> Serde<T> getSerde(Class<T> recordClass) {
        Serializer<T> serializer = (String topic, T data) -> {
            try {
                String jsonString = objectMapper.writeValueAsString(data);
                return jsonString.getBytes();
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };

        Deserializer<T> deserializer = (String topic, byte[] bytes) -> {
            try {
                if (bytes == null) return null;
                String jsonString = new String(bytes);  // Decode bytes to string
                return objectMapper.readValue(jsonString, recordClass);  // Parse JSON string
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing object", e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
