package com.chenwen.flink.window;

import common.SensorReading;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;

public class WindowTest {

    StreamExecutionEnvironment streamExecutionEnvironment;

    @Before
    public void addSource () {
        streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @Test
    public void test() throws Exception {
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = streamExecutionEnvironment.addSource(new SourceFunction<SensorReading>() {
            @Override
            public void run(SourceContext<SensorReading> sourceContext) throws Exception {
                for (int i = 0; i < 5; i++) {
                    for (int j = 0; j < 3; j++) {
                        sourceContext.collect(new SensorReading(0, System.currentTimeMillis(), 1d));
                        sourceContext.collect(new SensorReading(1, System.currentTimeMillis(), 2d));
                        sourceContext.collect(new SensorReading(2, System.currentTimeMillis(), 3d));
//                        Thread.sleep(TimeUnit.SECONDS.toSeconds(1000));
                    }
                    Thread.sleep(TimeUnit.SECONDS.toSeconds(5000));
                }
            }

            @Override
            public void cancel() {

            }
        });
        SingleOutputStreamOperator<Tuple2<Integer, Double>> sum = sensorReadingDataStreamSource.map(new MapFunction<SensorReading, Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Integer, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1);
        sum.print();
        streamExecutionEnvironment.execute();
    }



}
