import core.AbstractFlinkTask;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Collection;

class KMeans extends AbstractFlinkTask {

    private String pointsPath;
    private String centroidsPath;

    KMeans(ExecutionEnvironment executionEnvironment) {
        super(executionEnvironment);
    }

    void setPointsPath(String pointsPath) {
        this.pointsPath = pointsPath;
    }

    void setCentroidsPath(String centroidsPath) {
        this.centroidsPath = centroidsPath;
    }

    @Override
    public String getTaskExecutionDescription() {
        return super.getTaskExecutionDescription() + " POINTSPATH CENTROIDSPATH";
    }

    public void execute() throws Exception {

        // /root/flink/bin/flink run FILENAME.jar hdfs:///twitter/twitter_follower_pursuer_count.net hdfs:///twitter/twitter_centroids.net hdfs:///twitter/twitter_kmeans

        if (pointsPath.isEmpty()) {
            throw new IllegalArgumentException("The pointsPath is empty.");
        }

        DataSet<Point> points = executionEnvironment.readTextFile(pointsPath).flatMap(new PointSplitter());

        if (centroidsPath.isEmpty()) {
            throw new IllegalArgumentException("The centroidsPath is empty.");
        }

        DataSet<Centroid> centroids = executionEnvironment.readTextFile(centroidsPath).flatMap(new CentroidsSplitter());

        // set number of bulk iterations for KMeans algorithm
        IterativeDataSet<Centroid> loop = centroids.iterate(10);

        DataSet<Centroid> newCentroids = points
                // compute closest centroid for each point
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        // feed new centroids back into next iteration
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        // assign points to final clusters
        DataSet<Tuple2<Integer, Point>> clusteredPoints = points.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        if (outputPath.isEmpty()) {
            throw new IllegalArgumentException("The outpath is empty.");
        }

        clusteredPoints.writeAsCsv(outputPath + "_points.net", "\n", " ").setParallelism(1);
        finalCentroids.map(new CentroidTuple3Mapper()).writeAsCsv(outputPath + "_centroids.net", "\n", " ").setParallelism(1);

        executeEnvironment();
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    /**
     * A simple two-dimensional point.
     */
    public static class Point implements Serializable {

        double x, y;

        Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        Point add(Point point) {
            x += point.x;
            y += point.y;
            return this;
        }

        Point div(long divisor) {
            x /= divisor;
            y /= divisor;
            return this;
        }

        double euclideanDistance(Point point) {
            return Math.sqrt((x - point.x) * (x - point.x) + (y - point.y) * (y - point.y));
        }

        @Override
        public String toString() {
            return x + " " + y;
        }
    }

    /**
     * A point with an ID.
     */
    public static class Centroid extends Point {

        int id;

        Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }

        Centroid(int id, Point point) {
            super(point.x, point.y);
            this.id = id;
        }

        @Override
        public String toString() {
            return id + " " + super.toString();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * Determines the closest cluster center for a data point.
     */
    @ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        /**
         * Reads the centroid values from a broadcast variable into a collection.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /**
     * Appends a count variable to the tuple.
     */
    @ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /**
     * Sums and counts point coordinates.
     */
    @ForwardedFields("0")
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /**
     * Computes new centroid from coordinate sum and count of points.
     */
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.div(value.f2));
        }
    }

    private static class PointSplitter implements FlatMapFunction<String, Point> {
        @Override
        public void flatMap(String line, Collector<Point> out) {
            String[] arr = line.split("\\s+");
            out.collect(new Point(Integer.parseInt(arr[1]), Integer.parseInt(arr[2]))); // skip the accountId in the first column arr[0]
        }
    }

    private class CentroidsSplitter implements FlatMapFunction<String, Centroid> {
        @Override
        public void flatMap(String line, Collector<Centroid> out) throws Exception {
            String[] arr = line.split("\\s+");
            out.collect(new Centroid(Integer.parseInt(arr[0]), Double.parseDouble(arr[1]), Double.parseDouble(arr[2])));
        }
    }

    private static class CentroidTuple3Mapper implements MapFunction<Centroid, Tuple3<Integer, Double, Double>> {
        @Override
        public Tuple3<Integer, Double, Double> map(Centroid centroid) throws Exception {
            return new Tuple3<>(centroid.id, centroid.x, centroid.y);
        }
    }
}
