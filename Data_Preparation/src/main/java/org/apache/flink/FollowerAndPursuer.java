package org.apache.flink;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

@SuppressWarnings("unchecked")
class FollowerAndPursuer {

    static void countFollowerAndPursuer(ExecutionEnvironment env, String outputPath) throws Exception {
        DataSet<Tuple2<Integer, Integer>> dsGraph = env.readTextFile("hdfs:///twitter/twitter_rv.net")
                .flatMap((FlatMapFunction<String, Tuple2<Integer, Integer>>) (line, collector) -> collector.collect(new Tuple2<>(
                        Integer.parseInt(line.split("\\s+")[0]),
                        Integer.parseInt(line.split("\\s+")[1])
                )));

        DistinctOperator<Tuple2<Integer, Integer>> dsAccountIds = dsGraph.flatMap((FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>) (tuple, collector) -> {
            collector.collect(new Tuple2<>(tuple.f0, 0));
            collector.collect(new Tuple2<>(tuple.f1, 0));
        }).distinct();

        // count follower
        DataSet<Tuple2<Integer, Integer>> dsFollowerCount = mapAndGroupBy(dsGraph, new followerMapper(), dsAccountIds);

        dsFollowerCount.writeAsFormattedText(
                "hdfs:///twitter/follower_count.net",
                new OutputFormatter2Integers()
        ).setParallelism(1);


        // count pursuer
        DataSet<Tuple2<Integer, Integer>> dsPursuerCount = mapAndGroupBy(dsGraph, new pursuerMapper(), dsAccountIds);

        dsPursuerCount.writeAsFormattedText(
                "hdfs:///twitter/pursuer_count.net",
                new OutputFormatter2Integers()
        ).setParallelism(1);

        // join follower and pursuer count
        DataSet<Tuple3<Integer, Integer, Integer>> dsFollowerAndPursuerCount = dsFollowerCount.join(dsPursuerCount)
                .where(0)
                .equalTo(0)
                .projectFirst(0, 1)
                .projectSecond(1);

        dsFollowerAndPursuerCount.writeAsFormattedText(
                outputPath,
                new OutputFormatter3Integers()
        ).setParallelism(1);

        env.execute();
    }

    /**
     * map the graph tuple to a pursuer
     */
    private static class pursuerMapper implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> tuple) throws Exception {
            return new Tuple2<>(tuple.f0, 1);
        }
    }

    /**
     * map the graph tuple to a follower
     */
    private static class followerMapper implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> tuple) throws Exception {
            return new Tuple2<>(tuple.f1, 1);
        }
    }

    private static JoinOperator mapAndGroupBy(DataSet<Tuple2<Integer, Integer>> dataSet, MapFunction mapFunction, DataSet<Tuple2<Integer, Integer>> dsAccountIds) {
        return dataSet
                .map(mapFunction)
                .groupBy(0)
                .sum(1).rightOuterJoin(dsAccountIds).where(0).equalTo(0).with((FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>) (leftTuple, rightTuple, collector) -> {
                    if (leftTuple != null) {
                        collector.collect(new Tuple2<>(leftTuple.f0, leftTuple.f1));
                    } else {
                        collector.collect(rightTuple);
                    }
                });
    }

    private static class OutputFormatter2Integers implements TextOutputFormat.TextFormatter<Tuple2<Integer, Integer>> {
        @Override
        public String format(Tuple2<Integer, Integer> tuple) {
            return tuple.f0 + " " + tuple.f1;
        }
    }

    private static class OutputFormatter3Integers implements TextOutputFormat.TextFormatter<Tuple3<Integer, Integer, Integer>> {
        @Override
        public String format(Tuple3<Integer, Integer, Integer> tuple) {
            return tuple.f0 + " " + tuple.f1 + " " + tuple.f2;
        }
    }
}
