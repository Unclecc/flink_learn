package com.chenwen.flink.source;

import common.SensorReading;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerCallBridge010;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;

/**
 * @author chenwen
 */
public class SourceLearn {

    public static void main(String[] args) throws Exception {
//            readFromCollection();
//        readFromFile();
//        readFromKafka();
        customizeSource();
    }

    private static void readFromKafka() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> sensorSource = executionEnvironment.addSource(new FlinkKafkaConsumer010<String>(
                "sensor",
                new SimpleStringSchema(),
                properties
        ));

        sensorSource.map(str -> new SensorReading(
                        Integer.parseInt(str.split(", ")[0]),
                        Long.parseLong(str.split(", ")[1]),
                        Double.parseDouble(str.split(", ")[2])))
                .print();
        executionEnvironment.execute();
    }

    private static void readFromCollection() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> dataStreamSource = executionEnvironment.fromCollection(Arrays.asList(
                new SensorReading(1, 1L, 31.6),
                new SensorReading(2, 2L, 32.6),
                new SensorReading(3, 3L, 33.6),
                new SensorReading(4, 4L, 34.6),
                new SensorReading(5, 5L, 35.6)
        ));

        dataStreamSource.print();
        executionEnvironment.execute();
    }

    private static void readFromFile() throws Exception {
        URL url = SourceLearn.class.getClassLoader().getResource("SensorReading.txt");
        String path = url.getPath();
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = executionEnvironment
                .readFile(new TextInputFormat(new Path(url.toURI())), path);

        stringDataStreamSource.map(str -> new SensorReading(
                        Integer.parseInt(str.split(", ")[0]),
                        Long.parseLong(str.split(", ")[1]),
                        Double.parseDouble(str.split(", ")[2])))
                .print();
        executionEnvironment.execute();
    }

    public static void customizeSource() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.addSource(new SourceFunction<SensorReading>() {

                    private Boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
                        for (int i = 0; i < 5; i++) {
                            for (int j = 0; j < 5; j++) {
                                Integer id = random.nextInt(5);
                                Long timestamp = System.currentTimeMillis();
                                Double temperature = random.nextDouble() + random.nextInt(35);
                                sourceContext.collect(new SensorReading(
                                        id,
                                        timestamp,
                                        temperature
                                ));
                            }
                            Thread.sleep(5000);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .print();
        executionEnvironment.execute();
    }


}
