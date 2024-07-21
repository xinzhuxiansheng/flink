package com.yzhou.blog.wordcount;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class StreamWordCountMultipleSourceVariation {
    private static Logger logger = LoggerFactory.getLogger(StreamWordCountMultipleSourceVariation.class);
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        // 2. 创建多个 Socket 数据源
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 7777);
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 8888);

        // 3. 数据源2转换并生成中间数据流
        SingleOutputStreamOperator<Tuple2<String, Long>> source2Transformed = source2
                .flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .setParallelism(2);

        // 4. 合并 source1 和 source2Transformed
        DataStream<String> source1AndTransformedSource2 = source1.union(source2Transformed.map(t -> t.f0).returns(Types.STRING));

//        // 5. 创建 source3 数据流
//        DataStreamSource<String> source3 = env.socketTextStream("localhost", 9999);
//
//        // 6. 合并 source1AndTransformedSource2 和 source3
//        DataStream<String> mergedSources = source1AndTransformedSource2.union(source3);

        // 7. 转换最终合并后的数据流
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = source1AndTransformedSource2
                .flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .setParallelism(2);

        // 8. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

        // 9. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1).setParallelism(1).uid("wc-sum");

        // 10. 打印
        result.print();

        // 11. 执行
        env.execute();
    }
}
