import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

/**
 * inspired from https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/ml/JavaKMeansExample.java
 */
public class KMeansTask {

    static void execute(SparkSession sparkSession, int k ,String outputPath) {

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        String path = "hdfs:///twitter/twitter_follower_pursuer_count.net";
        JavaRDD<String> data = javaSparkContext.textFile(path);
        JavaRDD<Vector> parsedData = data.map(s -> {

            String[] tmpLine = s.split("\\s+");
            double[] values = new double[tmpLine.length];

            for (int i = 0; i < tmpLine.length; i++) {
                values[i] = Double.parseDouble(tmpLine[i]);
            }
            return Vectors.dense(values);

        });
        parsedData.cache();

        int numIterations = 10;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), k, numIterations, "random");

        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }


        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        RDD<Object> result = clusters.predict(parsedData.rdd());
        result.saveAsTextFile(outputPath);


        javaSparkContext.stop();

    }
}
