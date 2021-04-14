package core;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public abstract class AbstractFlinkTask implements IExecutableFlinkTask {

    protected ExecutionEnvironment executionEnvironment;
    public String outputPath;

    protected AbstractFlinkTask(ExecutionEnvironment executionEnvironment) {
        this.executionEnvironment = executionEnvironment;
    }

    public String getTaskExecutionDescription() {
        return this.getClass().getName();
    }

    protected void executeEnvironment() {
        try {
            executionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class LineSplitter2Integers implements FlatMapFunction<String, Tuple2<Integer, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<Integer, Integer>> out) {
            String[] arr = line.split("\\s+");
            out.collect(new Tuple2<>(Integer.parseInt(arr[0]), Integer.parseInt(arr[1])));
        }
    }

    public class LineSplitter3Integers implements FlatMapFunction<String, Tuple3<Integer, Integer, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<Integer, Integer, Integer>> out) {
            String[] arr = line.split("\\s+");
            out.collect(new Tuple3<>(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), Integer.parseInt(arr[2])));
        }
    }

    public class LineSplitterIntegerDouble implements FlatMapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<Integer, Double>> out) {
            String[] arr = line.split("\\s+");
            out.collect(new Tuple2<>(Integer.parseInt(arr[0]), Double.parseDouble(arr[1])));
        }
    }

    public class TextFormatter<T> implements TextOutputFormat.TextFormatter<T> {
        @Override
        public String format(T value) {
            return value.toString();
        }
    }

    public class TextFormatter2<T1, T2> implements TextOutputFormat.TextFormatter<Tuple2<T1, T2>> {
        @Override
        public String format(Tuple2<T1, T2> value) {
            return value.f0 + " " + value.f1;
        }
    }
}
