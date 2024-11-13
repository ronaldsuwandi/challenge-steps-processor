package com.ronaldsuwandi.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.NavigableMap;
import java.util.TreeMap;

public class NavigableMapSerde implements Serde<NavigableMap<Long, String>> {
    private final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<TreeMap<Long, String>> TYPE_REF = new TypeReference<>() {};
    @Override
    public Serializer<NavigableMap<Long, String>> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing NavigableMap", e);
            }
        };
    }

    @Override
    public Deserializer<NavigableMap<Long, String>> deserializer() {
        return (topic, bytes) -> {
            try {
                return mapper.readValue(bytes, TYPE_REF);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing NavigableMap", e);
            }
        };
    }
}
