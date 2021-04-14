import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class GrepTask {
    static void execute(SparkSession sparkSession, String substringSearch, String outputPath) {

        Tuple2<String, Integer> counts = sparkSession
                .sparkContext()
                .textFile("hdfs:///twitter/twitter_rv.net", 1)
                .toJavaRDD()
                .mapToPair(s -> new Tuple2<>("occurrence", StringUtils.countMatches(s, substringSearch)))
                .reduce(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> stringIntegerTuple22) throws Exception {
                        return new Tuple2<>(stringIntegerTuple2._1, stringIntegerTuple2._2 + stringIntegerTuple22._2);
                    }
                });

        List<String> myList = Collections.singletonList(counts.toString());
        Dataset<Row> df = sparkSession.createDataset(myList, Encoders.STRING()).toDF();
        df.write().mode(SaveMode.Overwrite).format("json").save(outputPath);

        System.out.println(counts.toString());
        sparkSession.stop();
    }
}
