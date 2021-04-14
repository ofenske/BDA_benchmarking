import core.AbstractFlinkTask;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceEdgesFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.PageRankAlgorithm;
import org.apache.flink.util.Collector;

class PageRankTask extends AbstractFlinkTask {

    private int maxIterations;
    private double dampeningFactor;

    PageRankTask(ExecutionEnvironment executionEnvironment) {
        super(executionEnvironment);
    }

    void setMaxIterations(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    void setDampeningFactor(double dampeningFactor) {
        this.dampeningFactor = dampeningFactor;
    }

    @Override
    public String getTaskExecutionDescription() {
        return super.getTaskExecutionDescription() + " MAXITERATIONS DAMPENINGFACTOR";
    }

    public void execute() throws Exception {

        executionEnvironment.getConfig().enableObjectReuse();

        DataSet<Tuple3<String, String, Double>> dsEdges = executionEnvironment.readTextFile("hdfs:///twitter/twitter_rv.net")
                .flatMap((FlatMapFunction<String, Tuple3<String, String, Double>>) (line, collector) -> {
                    String[] arr = line.split("\\s+");
                    collector.collect(new Tuple3<>(arr[0], arr[1], 1d));
                });

        DataSet<Tuple2<String, Double>> dsVertex = dsEdges.flatMap((FlatMapFunction<Tuple3<String, String, Double>, Tuple2<String, Double>>) (stringStringDoubleTuple3, collector) -> {
            collector.collect(new Tuple2<>(stringStringDoubleTuple3.f0, Double.parseDouble(stringStringDoubleTuple3.f0)));
            collector.collect(new Tuple2<>(stringStringDoubleTuple3.f1, Double.parseDouble(stringStringDoubleTuple3.f1)));
        }).distinct();

        dsVertex = dsVertex.map((MapFunction<Tuple2<String, Double>, Tuple2<String, Double>>) stringDoubleTuple2 -> new Tuple2<>(stringDoubleTuple2.f0, 1d));

        Graph<String, Double, Double> network = Graph.fromTupleDataSet(dsVertex, dsEdges, executionEnvironment);

        DataSet<Tuple2<String, Double>> sumEdgeWeights = network.reduceOnEdges((ReduceEdgesFunction<Double>) (aDouble, ev1) -> aDouble + ev1, EdgeDirection.OUT);

        Graph<String, Double, Double> networkWithWeights = network.joinWithEdgesOnSource(sumEdgeWeights, (MapFunction<Tuple2<Double, Double>, Double>) doubleDoubleTuple2 -> doubleDoubleTuple2.f0 / doubleDoubleTuple2.f1);

        if (maxIterations < 1) {
            throw new IllegalArgumentException("The maxIterations is smaller than 1.");
        }

        if (dampeningFactor > 1 || dampeningFactor < 0) {
            throw new IllegalArgumentException("The dampeningFactor is greater than 0 or smaller than 1.");
        }

        PageRankAlgorithm pageRankAlgorithm = new PageRankAlgorithm<String>(dampeningFactor, maxIterations);
        Graph<String, Double, Double> pageRankResultNetwork = pageRankAlgorithm.run(networkWithWeights);

        if (outputPath.isEmpty()) {
            throw new IllegalArgumentException("The outpath is empty.");
        }

        pageRankResultNetwork.getVertices().writeAsFormattedText(
                outputPath,
                (TextOutputFormat.TextFormatter<Vertex<String, Double>>) stringDoubleVertex -> stringDoubleVertex.f0 + " " + stringDoubleVertex.f1
        ).setParallelism(1);

        executeEnvironment();
    }
}