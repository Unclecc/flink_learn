package com.chenwen.flink.transform;

import common.SensorReading;
import java.util.Arrays;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

public class TransformTest {

  StreamExecutionEnvironment executionEnvironment;

  @Before
  public void setup() {
    executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
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

//  @Test
//  public void keyBy() throws Exception {
//    DataStreamSource<Object> dataStreamSource = executionEnvironment.fromCollection(
//        Arrays.asList(
//            new SensorReading(1, System.currentTimeMillis(), 34d),
//            new SensorReading(2, System.currentTimeMillis(), 36d),
//            new SensorReading(1, System.currentTimeMillis(), 30d),
//            new SensorReading(2, System.currentTimeMillis(), 32d)
//        )
//    );
//    dataStreamSource.keyBy(new KeySelector<SensorReading, Integer>() {
//      @Override
//      public Integer getKey(SensorReading sensorReading) throws Exception {
//        return 1;
//      }
//    }).print();
//    executionEnvironment.execute();
//  }

}

















