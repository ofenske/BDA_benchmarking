import core.AbstractFlinkTask;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

class JoinTask extends AbstractFlinkTask {

    JoinTask(ExecutionEnvironment executionEnvironment) {
        super(executionEnvironment);
    }

    public void execute() throws Exception {
        DataSet<String> dsPageRankLines = executionEnvironment.readTextFile("hdfs:///twitter/twitter_pagerank_10_iteration_initial_pagerank_is_1.net");
        DataSet<String> dsWebLogLines = executionEnvironment.readTextFile("hdfs:///twitter/twitter_web_log.net");

        DataSet<Tuple2<Integer, Double>> dsPageRank = dsPageRankLines.flatMap(new LineSplitterIntegerDouble());
        DataSet<Tuple3<Integer, Integer, Integer>> dsWebLogFilteredByTimestamp = dsWebLogLines.flatMap(new LineSplitter3Integers()).filter(new TimestampFilter());
        // get DataSet with account-id, ip-address and most number of visited accounts
        DataSet<Tuple2<Integer, Integer>> dsWeblogIpAddressWithMostAccountsVisited = dsWebLogFilteredByTimestamp.groupBy(1).reduceGroup(new IPAdressGroupReduce()).maxBy(1);

        // filter web log entries which were visited by ip adress with most visits
        DataSet<Tuple3<Integer, Integer, Integer>> dsWebLogFilteredByTimestampAndIPAddress = dsWebLogFilteredByTimestamp.join(dsWeblogIpAddressWithMostAccountsVisited).where(1).equalTo(0).projectFirst(0, 2).projectSecond(0);

        DataSet<Tuple4<Integer, Double, Integer, Integer>> dsJoin = dsPageRank.join(dsWebLogFilteredByTimestampAndIPAddress)
                // key definition on first DataSet using a field position key
                .where(0) // account_id
                // key definition of second DataSet using a field position key
                .equalTo(0) // account_id
                // project on fields and calc the avg page rank for each account
                .projectFirst(0, 1).projectSecond(1, 2);

        // count the number of accounts
        long numberOfAccountsWithPageRank = dsJoin.count();

        // calc the sum of page ranks and divide it by the number of accounts to get the avg
        DataSet<Double> dsJoinWithPageRankAvg = dsJoin.reduceGroup(new PageRankSumGroupReduce()).sum(0).map(new AvgMap(numberOfAccountsWithPageRank));

        //write result to file
        //example 'hdfs:///twitter/join_task.net'
        dsJoinWithPageRankAvg.writeAsFormattedText(outputPath, new TextFormatter<>()).setParallelism(1);

        executeEnvironment();
    }

    private class TimestampFilter implements FilterFunction<Tuple3<Integer, Integer, Integer>> {
        /**
         * filter timestamps between 2016-12-01 00:00:00 and 2016-12-31 23:59:59
         */
        @Override
        public boolean filter(Tuple3<Integer, Integer, Integer> in) throws Exception {
            return in.f2 > 1480546800 && in.f2 < 1483225199;
        }
    }

    public class PageRankSumGroupReduce implements GroupReduceFunction<Tuple4<Integer, Double, Integer, Integer>, Tuple1<Double>> {
        @Override
        public void reduce(Iterable<Tuple4<Integer, Double, Integer, Integer>> in, Collector<Tuple1<Double>> out) {
            Double pageRankSum = 0.;

            for (Tuple4<Integer, Double, Integer, Integer> t : in) {
                pageRankSum += t.f1;
            }

            out.collect(new Tuple1<>(pageRankSum));
        }
    }

    public class IPAdressGroupReduce implements GroupReduceFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> {
        /**
         * get number of rows for grouped ip adresses
         */
        @Override
        public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> in, Collector<Tuple2<Integer, Integer>> out) {
            Set<Integer> uniqIPAdresses = new HashSet<>();
            Integer numberOfRows = 0;

            for (Tuple3<Integer, Integer, Integer> t : in) {
                uniqIPAdresses.add(t.f1);
                numberOfRows++;
            }

            for (Integer IPAdress : uniqIPAdresses) {
                out.collect(new Tuple2<>(IPAdress, numberOfRows));
            }
        }
    }

    /**
     * use an injected count and calc the avg by using the provide sum
     */
    private class AvgMap implements MapFunction<Tuple1<Double>, Double> {
        private long numberOfAccountsWithPageRank;

        AvgMap(long numberOfAccountsWithPageRank) {
            this.numberOfAccountsWithPageRank = numberOfAccountsWithPageRank;
        }

        @Override
        public Double map(Tuple1<Double> tuple1) throws Exception {
            return tuple1.f0 / numberOfAccountsWithPageRank;
        }
    }
}
