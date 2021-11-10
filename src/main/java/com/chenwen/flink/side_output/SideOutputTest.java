package com.chenwen.flink.side_output;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

public class SideOutputTest {
    static final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> dataStreamSource = executionEnvironment.fromCollection(Arrays.asList(
                1, 2, 3, 4, 5
        ));

        SingleOutputStreamOperator<Object> process = dataStreamSource.process(new ProcessFunction<Integer, Object>() {
            @Override
            public void processElement(Integer integer,
                                       ProcessFunction<Integer, Object>.Context context, Collector<Object> collector) throws Exception {
                collector.collect(integer);
                context.output(outputTag, integer + "");
            }
        });

        process.print();

        DataStream<String> sideOutput = process.getSideOutput(outputTag);
        sideOutput.map(str -> str + "siteOutput").print();

        executionEnvironment.execute();

    }

}
