package com.example.docgen;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SimpleFlinkApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.fromCollection(Arrays.asList(
            "hello flink",
            "flink is simple",
            "hello java"
        ));

        DataStream<Tuple2<String, Integer>> counts = text
            .flatMap(new Tokenizer())
            .keyBy(value -> value.f0)
            .sum(1);

        counts.print();

        env.execute("Simple Flink Java Application");
    }

    private static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String token : value.toLowerCase().split("\\W+")) {
                if (!token.isEmpty()) {
                    out.collect(Tuple2.of(token, 1));
                }
            }
        }
    }
}
