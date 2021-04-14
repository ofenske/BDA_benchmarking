import core.AbstractFlinkTask;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

class LineCounter extends AbstractFlinkTask {

    LineCounter(ExecutionEnvironment executionEnvironment) {
        super(executionEnvironment);
    }

    public void execute() {
        DataSet<Integer> count = executionEnvironment.readTextFile("hdfs:///twitter/twitter_rv.net").mapPartition(new PartitionCounter()).reduce(new IntegerSummer());

        //write result to file
        //example 'hdfs:///twitter/line_count.net'
        count.writeAsFormattedText(outputPath, new TextFormatter<>()).setParallelism(1);

        executeEnvironment();
    }

    private class PartitionCounter implements MapPartitionFunction<String, Integer> {

        public void mapPartition(Iterable<String> lines, Collector<Integer> out) {
            int c = 0;
            for (String line : lines) {
                c++;
            }
            out.collect(c);
        }
    }

    private class IntegerSummer implements ReduceFunction<Integer> {
        @Override
        public Integer reduce(Integer number1, Integer number2) {
            return number1 + number2;
        }
    }
}
