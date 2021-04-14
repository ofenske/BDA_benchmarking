package org.apache.flink;

import org.apache.flink.api.java.ExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {

        if (args.length != 2 || (!args[0].equals("-webLog") && !args[0].equals("-followerAndPursuer"))) {
            System.out.println();
            System.out.println(
                    "Usage: '/root/flink/bin/flink run Main.jar " +
                            "[OPTION] [OUTPUTPATH]"
            );

            System.out.println();
            System.out.println(
                    "[OPTION]:\n" +
                            "\t -webLog\n" +
                            "\t -followerAndPursuer\n"
            );

            System.out.println(
                    "[OUTPUTPATH]:\n" +
                            "\t example: hdfs:///example.txt"
            );

            System.exit(0);
        }


        final ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(3);

        switch (args[0]) {
            case "-webLog":
                WebLog.generateWebLog(executionEnvironment, args[1]);
                break;
            case "-followerAndPursuer":
                FollowerAndPursuer.countFollowerAndPursuer(executionEnvironment, args[1]);
                break;
            default:
                System.out.println("Nothing to do yet!");
                System.exit(0);
        }
    }
}
