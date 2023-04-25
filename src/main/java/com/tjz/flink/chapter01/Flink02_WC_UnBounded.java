package com.tjz.flink.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_WC_UnBounded {
    public static void main(String[] args) throws Exception {
    // 1. 创建流的这些环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    // 2. 通过环境从source获取一个流（source）
        DataStreamSource<String> source = env.socketTextStream("hadoop102",8888);
    // 3. 对流做各种转换（transform）
        SingleOutputStreamOperator<String> wordStream = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(word);
                }

            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> wordoneStream = wordStream.map(new MapFunction<String, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });

        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordoneStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {

            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = keyedStream.sum(1);
     // 4. 输出（sink）
        resultStream.print();


    // 5. 执行环境
        env.execute();
    }
}
