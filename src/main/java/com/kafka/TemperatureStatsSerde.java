package com.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class TemperatureStatsSerde implements Serde<TraitementTemperature.TemperatureStats> {

    @Override
    public Serializer<TraitementTemperature.TemperatureStats> serializer() {
        return new TemperatureStatsSerializer();
    }

    @Override
    public Deserializer<TraitementTemperature.TemperatureStats> deserializer() {
        return new TemperatureStatsDeserializer();
    }

    public static class TemperatureStatsSerializer implements Serializer<TraitementTemperature.TemperatureStats> {
        @Override
        public byte[] serialize(String topic, TraitementTemperature.TemperatureStats data) {
            if (data == null) {
                return null;
            }
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putDouble(data.sum);
            buffer.putLong(data.count);
            return buffer.array();
        }
    }

    public static class TemperatureStatsDeserializer implements Deserializer<TraitementTemperature.TemperatureStats> {
        @Override
        public TraitementTemperature.TemperatureStats deserialize(String topic, byte[] data) {
            if (data == null || data.length != 16) {
                return new TraitementTemperature.TemperatureStats();
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);
            double sum = buffer.getDouble();
            long count = buffer.getLong();
            return new TraitementTemperature.TemperatureStats(sum, count);
        }
    }
}
