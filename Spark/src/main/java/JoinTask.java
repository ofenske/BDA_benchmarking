import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

class JoinTask {

    static void execute(SparkSession sparkSession, String outputPath) {

        JavaRDD<PageRank> rddPageRank = sparkSession.read().textFile("hdfs:///twitter/twitter_pagerank.net").javaRDD()
                .map((Function<String, PageRank>) line -> {
                    String[] arr = line.split("\\s+");
                    return new PageRank(Integer.parseInt(arr[0]), Double.parseDouble(arr[1]));
                });

        Dataset<Row> dsPageRank = sparkSession.createDataFrame(rddPageRank, PageRank.class);

        JavaRDD<WebLog> rddWebLog = sparkSession.read().textFile("hdfs:///twitter/twitter_web_log.net").javaRDD()
                .map((Function<String, WebLog>) line -> {
                    String[] arr = line.split("\\s+");
                    return new WebLog(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), Integer.parseInt(arr[2]));
                });

        // filter timestamps between 2016-12-01 00:00:00 and 2016-12-31 23:59:59
        Dataset<Row> dsWebLogFilteredByTimestamp = sparkSession.createDataFrame(rddWebLog, WebLog.class).filter(col("timestamp").gt(1480546800)).filter(col("timestamp").lt(1483225199));

        // the count() implicitly adds the column "count" to the dataset
        String ipAdressWithMostAccountsVisited = getStringFromSingleColumnRow(dsWebLogFilteredByTimestamp.groupBy("ipAdress").count().orderBy(col("count").desc()).limit(1).select("ipAdress"));

        Dataset<Row> dsWebLogFilteredByTimestampAndIpAdress = dsWebLogFilteredByTimestamp.where("ipAdress = " + ipAdressWithMostAccountsVisited);

        System.out.println("filtered WebLog by Timestamp and ipAdress with the most accounts visited");

        Dataset<Row> dsJoin = dsPageRank.join(dsWebLogFilteredByTimestampAndIpAdress, "accountId");

        Dataset<Row> dsPageRankAvg = dsJoin.select("pageRank").agg(avg("pageRank"));

        String pageRankAvg = getStringFromSingleColumnRow(dsPageRankAvg);

        System.out.println("avg(pageRank) = " + pageRankAvg);

        dsPageRankAvg.write().mode(SaveMode.Overwrite).format("json").save(outputPath);

        sparkSession.stop();
    }

    private static String getStringFromSingleColumnRow(Dataset<Row> dataset) {
        return dataset.map(Row::mkString, Encoders.STRING()).collectAsList().get(0);
    }
}
