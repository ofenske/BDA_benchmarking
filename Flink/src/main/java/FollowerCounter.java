import core.AbstractFlinkTask;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

class FollowerCounter extends AbstractFlinkTask {

    FollowerCounter(ExecutionEnvironment executionEnvironment) {
        super(executionEnvironment);
    }

    public void execute() {

        DataSet<String> dsTextLines = executionEnvironment.readTextFile("hdfs:///twitter/twitter_rv.net");
        DataSet<Tuple2<Integer, Integer>> dsFollowerCount = dsTextLines
                .flatMap(new LineSplitter2Integers())
                .sortPartition(0, Order.ASCENDING)
                .groupBy(0)
                .sum(1);

        //write result to file
        //example 'hdfs:///twitter/follower_count.net'
        dsFollowerCount.writeAsFormattedText(outputPath, new TextFormatter2<>()).setParallelism(1);

        executeEnvironment();
    }
}
