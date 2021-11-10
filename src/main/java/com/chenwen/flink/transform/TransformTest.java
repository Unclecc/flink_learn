package com.chenwen.flink.transform;

import common.SensorReading;

import java.util.*;

import common.SmokeLevel;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

public class TransformTest {

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
    public void map() throws Exception {
        DataStreamSource<Integer> dataStreamSource = executionEnvironment.fromCollection(
                Arrays.asList(1, 2, 3, 4)
        );
        dataStreamSource.map(num -> num * 2).print();
        executionEnvironment.execute();
    }

    @Test
    public void flatMap() throws Exception {
        DataStreamSource<String> dataStreamSource = executionEnvironment.fromCollection(
                Arrays.asList("a b", "c d")
        );
        dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
                                     @Override
                                     public void flatMap(String s, Collector<String> collector) throws Exception {
                                         String[] s1 = s.split(" ");
                                         Arrays.stream(s1).forEach(str -> collector.collect(str));
                                     }
                                 }
        ).print();
        executionEnvironment.execute();
    }

    @Test
    public void filter() throws Exception {
        DataStreamSource<Integer> dataStreamSource = executionEnvironment.fromCollection(
                Arrays.asList(1, 2, 3, 4)
        );
        dataStreamSource.filter(num -> num > 2).print();
        executionEnvironment.execute();
    }

    @Test
    public void keyBy() throws Exception {
        DataStreamSource<SensorReading> dataStreamSource = executionEnvironment.fromCollection(
                Arrays.asList(
                        new SensorReading(1, System.currentTimeMillis(), 34d),
                        new SensorReading(2, System.currentTimeMillis(), 36d),
                        new SensorReading(1, System.currentTimeMillis(), 30d),
                        new SensorReading(2, System.currentTimeMillis(), 32d)
                )
        );

//        SingleOutputStreamOperator<SensorReading> sum = dataStreamSource.keyBy("id").sum("temperature");
        SingleOutputStreamOperator<SensorReading> max = dataStreamSource.keyBy("id").maxBy("temperature");
        max.print();
        executionEnvironment.execute();
    }



    @Test
    public void reduce() throws Exception {
        SingleOutputStreamOperator<SensorReading> reduce = dataStreamSource.keyBy("id").reduce((ReduceFunction<SensorReading>) (sensorReading, t1) -> {
            sensorReading.setTemperature(sensorReading.getTemperature() + t1.getTemperature());
            sensorReading.setTimestamp(System.currentTimeMillis());
            return sensorReading;
        });
        reduce.print();
        executionEnvironment.execute();
    }

    @Test
    public void split() throws Exception {
        SplitStream<SensorReading> split = dataStreamSource.split((OutputSelector<SensorReading>) sensorReading -> {
            List<String> select = new ArrayList<>();
            if (sensorReading.getId() == 1) {
                select.add("sensor1");
            } else {
                select.add("sensor2");
            }
            return select;
        });
        DataStream<SensorReading> sensor1 = split.select("sensor1");
        sensor1.print("sensor1");
        executionEnvironment.execute();
    }

    /**
     * 联合两条流的事件是非常常见的流处理需求。
     * 例如监控一片森林然后发出高危的火警警报。
     * 报警的Application接收两条流，一条是温度传感器传回来的数据，一条是烟雾传感器传回来的数据。当两条流都超过各自的阈值时，报警。
     */
    @Test
    public void connectAndCoMap() throws Exception {
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment.addSource(new SourceFunction<SensorReading>() {
            private int count = 100;
            Random random = new Random();

            @Override
            public void run(SourceContext<SensorReading> sourceContext) throws Exception {
                while (count > 0) {
                    sourceContext.collect(new SensorReading(1, System.currentTimeMillis(), random.nextInt(20) + 90D));
                    Thread.sleep(100);
                    count--;
                }
            }

            @Override
            public void cancel() {

            }
        });

        DataStreamSource<SmokeLevel> smokeLevelDataStreamSource = executionEnvironment.addSource(new SourceFunction<SmokeLevel>() {
            private int count = 100;
            Random random = new Random();

            @Override
            public void run(SourceContext<SmokeLevel> sourceContext) throws Exception {
                while (count > 0) {
                    if (random.nextGaussian() > 0.8) {
                        sourceContext.collect(SmokeLevel.HIGH);
                    } else {
                        sourceContext.collect(SmokeLevel.LOW);
                    }
                    Thread.sleep(100);
                    count--;
                }
            }

            @Override
            public void cancel() {

            }
        });

        ConnectedStreams<SensorReading, SmokeLevel> connect = sensorReadingDataStreamSource.connect(smokeLevelDataStreamSource.broadcast());
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = connect.flatMap(new CoFlatMapFunction<SensorReading, SmokeLevel, String>() {

            private SmokeLevel smokeLevel;

            @Override
            public void flatMap1(SensorReading sensorReading, Collector<String> collector) throws Exception {
                if (sensorReading.getTemperature() > 100 && this.smokeLevel != null && this.smokeLevel == SmokeLevel.HIGH) {
                    collector.collect("Risk of fire:" + sensorReading.getTemperature() + "--" + new Date());
                }
            }

            @Override
            public void flatMap2(SmokeLevel smokeLevel, Collector<String> collector) throws Exception {
                this.smokeLevel = smokeLevel;
            }
        });

        stringSingleOutputStreamOperator.print();
        executionEnvironment.execute();
    }

    /**
     * union
     */
    @Test
    public void union() throws Exception {
        DataStreamSource<Integer> dataStreamSource1 = executionEnvironment.fromCollection(
                Arrays.asList(1, 2, 3, 4)
        );

        DataStreamSource<Integer> dataStreamSource2 = executionEnvironment.fromCollection(
                Arrays.asList(5, 6, 7, 8)
        );

        DataStream<Integer> union = dataStreamSource1.union(dataStreamSource2);
        union.print();
        executionEnvironment.execute();
    }

}


















