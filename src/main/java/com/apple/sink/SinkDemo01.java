package com.apple.sink;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @Program: flink-java
 * @ClassName: SinkDemo01
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-09-17 13:54
 * @Version 1.1.0
 **/
public class SinkDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //在Flink1.12.0有个重要的功能，批流一体，就是可以用之前流处理的API处理批数据。
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> inputStream = env.readTextFile("data/input/words.txt");
        //inputStream.print();
        //inputStream.print("输出标志");
        //会在控制台上以红色输出
        //inputStream.printToErr();
        //会在控制台上以红色输出
        //inputStream.printToErr("输出标识");
        //inputStream.writeAsText("data/output/result1").setParallelism(1);
        //inputStream.writeAsText("data/output/result2").setParallelism(2);


        //inputStream.addSink(new MySinkFunction());
        String outputPath = "data/output/result3";

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        inputStream.addSink(sink).setParallelism(1);

        env.execute();
    }

//    private static class MySinkFunction extends RichSinkFunction<String> {
//        String filePath = "data/output/result3";
//
//        @Override
//        public void invoke(String value, Context context) {
//            System.out.println(":" + value);
//            new FileUtil().RecordToFile(value, filePath);
//        }
//    }
}
