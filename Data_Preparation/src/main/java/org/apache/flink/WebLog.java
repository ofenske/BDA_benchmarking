package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

class WebLog {

    static void generateWebLog(ExecutionEnvironment env, String outputPath) throws Exception {
        //read twitter file from hadoop
        DataSet<String> dsTextLines = env.readTextFile("hdfs:///twitter/twitter_rv.net");

        //get all data in a DataSet
        DataSet<Integer> dsIds = dsTextLines.flatMap(new InputLineSplitter());
        //get a unique data set
        DataSet<Integer> dsUniqueIds = dsIds.distinct();

        //generate random number between 0-100
        DataSet<Tuple3<Integer, Integer, Integer>> dsTupleGeneratedNumbers = dsUniqueIds.flatMap(new RandomNumberGenerator());
        DataSet<Tuple3<Integer, Integer, Integer>> dsRandom = dsTupleGeneratedNumbers.sortPartition(2, Order.ASCENDING);

        //write result to file
        //example 'hdfs:///twitter/twitter_weblog.net'
        dsRandom.writeAsFormattedText(
                outputPath,
                new TextOutputFormat.TextFormatter<Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public String format(Tuple3<Integer, Integer, Integer> value) {
                        return value.f0 + " " + value.f1 + " " + value.f2;
                    }
                }
        ).setParallelism(1);

        env.execute();
    }

    private static class InputLineSplitter implements FlatMapFunction<String, Integer> {
        @Override
        public void flatMap(String value, Collector<Integer> out) {
            for (String token : value.split("\\s+")) {
                out.collect(Integer.parseInt(token));
            }
        }
    }

    private static class RandomNumberGenerator implements FlatMapFunction<Integer, Tuple3<Integer, Integer, Integer>> {

        @Override
        public void flatMap(Integer value, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {

            for (int i = ThreadLocalRandom.current().nextInt(0, 9 + 1); i > 0; i--) {
                out.collect(
                        new Tuple3<>(
                                value,
                                ThreadLocalRandom.current().nextInt(1, 100 + 1), //random 'ip'
                                ThreadLocalRandom.current().nextInt(1451602800, 1483225199 + 1) //unix timestamp in 2016
                        ));
            }
        }
    }
}
