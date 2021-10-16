package com.apple.connectors;

import org.apache.commons.lang.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;


/**
 * @Program: flink-java
 * @ClassName: RedisDemo
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-09-17 17:26
 * @Version 1.1.0
 * https://blog.csdn.net/qq_31866793/article/details/93530244
 * https://www.cnblogs.com/qiu-hua/p/13794811.html
 * https://www.jianshu.com/p/807eef1f209d
 **/
public class RedisDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //env.setParallelism(1);
        System.setProperty("HADOOP_USER_NAME", "ubuntu");
        //开启检查点且指定检查点时间间隔 5000ms
        env.enableCheckpointing(5000);

        //配置检查点故障恢复与容错机制
        CheckpointConfig config = env.getCheckpointConfig();
        if (SystemUtils.IS_OS_WINDOWS) {
            config.setCheckpointStorage("file:///D:/IdeaProjcets/flink-java/data/ckp");
        } else {
            config.setCheckpointStorage("hdfs://master:9000/flink_1.13.1/checkpoint");
        }
        env.setRestartStrategy(RestartStrategies.noRestart());
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置两个检查点分割符生成间隔
        config.setCheckpointInterval(1000);
        //设置检查点生成模式：默认精确一次
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置检查点超时时间：即当某个检查点再某个时间段内还没生成完成就丢弃当前检查点
        config.setCheckpointTimeout(5000);
        //最大并行生成检查点个数（即同时可以有多少个检查点正在生成，默认一个）
        config.setMaxConcurrentCheckpoints(1);
        //前后两个检查点生成间隔时间，这与setMaxConcurrentCheckpoints(1)功能冲突，可以选择配置
        config.setMinPauseBetweenCheckpoints(200);
        //可容忍的检查点生成失败次数，超过这个次数丢弃掉当前检查点
        config.setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        DataStream<String> lines = env.socketTextStream("master", 9999);
        System.out.println("lines: " + lines.toString());
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                System.out.println("arr: " + arr[0]);
                for (String word : arr) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0).process(new MyKeyedProcessFunction());
        System.out.println("=================================");
        result.print();


        Set<InetSocketAddress> nodesSet = new HashSet<InetSocketAddress>();
        nodesSet.add(new InetSocketAddress("192.168.230.2", 6379));
        nodesSet.add(new InetSocketAddress("192.168.230.3", 6379));
        nodesSet.add(new InetSocketAddress("192.168.230.4", 6379));
        nodesSet.add(new InetSocketAddress("192.168.230.2", 6380));
        nodesSet.add(new InetSocketAddress("192.168.230.3", 6380));
        nodesSet.add(new InetSocketAddress("192.168.230.4", 6380));
        int connectionTimeout = 2000;
        int maxRedirections = 3;
        int maxTotal = 32;
        int maxIdle = 16;
        int minIdle = 8;
        FlinkJedisClusterConfig conf = new FlinkJedisClusterConfig.Builder()
                .setNodes(nodesSet)
                .setTimeout(connectionTimeout)
                .setMaxRedirections(maxRedirections)
                .setMaxTotal(maxTotal)
                .setMaxIdle(maxIdle)
                .setMinIdle(minIdle)
                .build();
        RedisSink<Tuple2<String, Integer>> redisSink = new RedisSink<Tuple2<String, Integer>>(conf, new MyRedisMapper());
        result.addSink(redisSink);
        env.execute("wc");
    }
}

class MyRedisMapper implements RedisMapper<Tuple2<String, Integer>> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "wcresult");
    }

    @Override
    public String getKeyFromData(Tuple2<String, Integer> data) {
        return data.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Integer> data) {
        return data.f1.toString();
    }
}

class MyKeyedProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
    private MapState<String, Integer> wcCountMapState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open: ");
        // 创建StateDescriptor
        wcCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>
                ("mapstate", String.class, Integer.class));
    }

    @Override
    public void processElement(Tuple2<String, Integer> tuple, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer value = 0;
        if (wcCountMapState.contains(tuple.f0)) {
            value = wcCountMapState.get(tuple.f0) + tuple.f1;
            System.out.println("wcCountMapState value " + value);
        } else {
            value = tuple.f1;
        }
        wcCountMapState.put(tuple.f0, value);
        System.out.println("processElement key: " + tuple.f0);
        System.out.println("processElement value: " + value);
        out.collect(new Tuple2<String, Integer>(tuple.f0, value));
    }
}
