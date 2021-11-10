package com.chenwen.flink.sink;

import common.SensorReading;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class SinkTest {

    StreamExecutionEnvironment executionEnvironment;
    DataStreamSource<SensorReading> dataStreamSource;
    @Before
    public void setup() {
        executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        dataStreamSource = executionEnvironment.fromCollection(
                Arrays.asList(
                        new SensorReading(1, System.currentTimeMillis(), 34d),
                        new SensorReading(2, System.currentTimeMillis(), 36d),
                        new SensorReading(1, System.currentTimeMillis(), 30d),
                        new SensorReading(2, System.currentTimeMillis(), 32d)
                )
        );
    }

    @Test
    public void testSinkKafka() throws Exception {
        SingleOutputStreamOperator<String> map = dataStreamSource.filter(sensorReading -> sensorReading.getId() == 1)
                .map(sensorReading -> sensorReading.toString());
        map.addSink(new FlinkKafkaProducer09<String>("localhost:9092", "sensor", new SimpleStringSchema()));
        executionEnvironment.execute();
    }

}
