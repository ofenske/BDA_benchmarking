import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        if (args.length < 2) {
            printUsageAndExit();
        }
        if (!args[0].equals("-grepTask")
                && !args[0].equals("-joinTask")
                && !args[0].equals("-kMeans")) {
            printUsageAndExit();
        }
        if ((args[0].equals("-grepTask") || args[0].equals("-kMeans")) && args.length != 3) {
            printUsageAndExit();
        }

        SparkConf conf = new SparkConf().setMaster("spark://paradisesrv:7077");
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark PArADISE")
                .config(conf)
                .getOrCreate();


        sparkSession.sparkContext().setLogLevel("ERROR");
        System.out.println("stop logging infos \n");

        long start = System.currentTimeMillis();

        System.out.println("Starting " + args[0] + " ... \n");

        switch (args[0]) {
            case "-grepTask":
                GrepTask.execute(sparkSession, args[1], args[2]);
                break;
            case "-joinTask":
                JoinTask.execute(sparkSession, args[1]);
                break;
            case "-kMeans":
                KMeansTask.execute(sparkSession, Integer.parseInt(args[1]), args[2]);
                break;
        }

        System.out.println("time: " + (System.currentTimeMillis() - start) + " ms");
    }

    private static void printUsageAndExit() {
        System.out.println();
        System.out.println(
                "Usage: '/root/spark/bin/spark-submit Spark.jar " +
                        "[OPTION] [OUTPUTPATH]"
        );
        System.out.println();
        System.out.println(
                "[OPTION]:\n" +
                        "\t -grepTask [SEARCHSUBSTRING]\n" +
                        "\t -joinTask\n" +
                        "\t -kMeans [NUMBEROFCLUSTER]"
        );
        System.out.println(
                "[OUTPUTPATH]:\n" +
                        "\t example: hdfs:///example.txt"
        );
        System.exit(0);
    }
}