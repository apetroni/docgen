package com.example.docgen;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SimpleFlinkApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1_000L);

        DataStream<String> text = env.fromCollection(Arrays.asList(
            "hello flink",
            "flink is simple",
            "hello java"
        ));

        DataStream<Tuple2<String, Integer>> counts = text
            .flatMap(new Tokenizer())
            .keyBy(value -> value.f0)
            .sum(1)
            .map(new StatisticsCheckpointListener());

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

    private static class StatisticsCheckpointListener extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>
        implements CheckpointedFunction, CheckpointListener {

        private static final Path STATS_FILE_PATH = Paths.get("job-statistics.txt");

        private transient ListState<Long> statsState;

        private long processedRecordCount;
        private long successfulCheckpointCount;

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) {
            processedRecordCount++;
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            statsState.clear();
            statsState.add(processedRecordCount);
            statsState.add(successfulCheckpointCount);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("job-stats", Long.class);
            statsState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                int index = 0;
                for (Long value : statsState.get()) {
                    if (index == 0) {
                        processedRecordCount = value;
                    } else if (index == 1) {
                        successfulCheckpointCount = value;
                    }
                    index++;
                }

            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            successfulCheckpointCount++;
            dumpStatistics(checkpointId, "COMPLETED");
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {
            dumpStatistics(checkpointId, "ABORTED");
        }

        private void dumpStatistics(long checkpointId, String checkpointStatus) throws IOException {
            String line = String.format(
                "%s checkpointId=%d status=%s processedRecords=%d successfulCheckpoints=%d%n",
                Instant.now(),
                checkpointId,
                checkpointStatus,
                processedRecordCount,
                successfulCheckpointCount
            );

            Files.write(
                STATS_FILE_PATH,
                Collections.singletonList(line),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            );
        }
    }
}
