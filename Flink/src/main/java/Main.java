import core.AbstractFlinkTask;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static ExecutionEnvironment executionEnvironment;

    private static List<AbstractFlinkTask> tasks;

    /**
     * provide instances of the tasks which contains executable Flink methods
     */
    private static void instantiateTasks() {
        executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        tasks = new ArrayList<>();
        tasks.add(new LineCounter(executionEnvironment));
        tasks.add(new GrepTask(executionEnvironment));
        tasks.add(new FollowerCounter(executionEnvironment));
        tasks.add(new JoinTask(executionEnvironment));
        tasks.add(new KMeans(executionEnvironment));
        tasks.add(new PageRankTask(executionEnvironment));
    }

    public static void main(String[] args) {
        instantiateTasks();

        if (args.length < 3) {
            printUsageAndExit("Please provide the necessary parameters.");
        }

        Integer parallelism = 0;

        try {
            parallelism = Integer.parseInt(args[0]);
        } catch (NumberFormatException exception) {
            printUsageAndExit(getStackTrace(exception));
        }

        String taskName = args[1];

        if (parallelism < 0) {
            printUsageAndExit("Please provide a positiv integer for parallelism.");
        } else {
            executionEnvironment.setParallelism(parallelism);
        }

        if (!taskExists(taskName)) {
            printUsageAndExit("Please provide a valid task name. " + taskName + " could not be found.");
        }

        AbstractFlinkTask task = getTaskFromArgs(taskName);

        if (task instanceof GrepTask) {
            GrepTask grepTask = (GrepTask) task;
            grepTask.setSearchString(args[2]);
            grepTask.outputPath = args[3];

            try {
                grepTask.execute();
            } catch (IllegalArgumentException exception) {
                printUsageAndExit(getStackTrace(exception));
            }
        } else if (task instanceof KMeans) {
            KMeans kMeans = (KMeans) task;
            kMeans.setPointsPath(args[2]);
            kMeans.setCentroidsPath(args[3]);
            kMeans.outputPath = args[4];

            try {
                kMeans.execute();
            } catch (Exception exception) {
                printUsageAndExit(getStackTrace(exception));
            }
        }  else if (task instanceof PageRankTask) {
            PageRankTask pageRankTask = (PageRankTask) task;
            pageRankTask.setMaxIterations(Integer.parseInt(args[2]));
            pageRankTask.setDampeningFactor(Double.parseDouble(args[3]));
            pageRankTask.outputPath = args[4];

            try {
                pageRankTask.execute();
            } catch (Exception exception) {
                printUsageAndExit(getStackTrace(exception));
            }
        } else {
            task.outputPath = args[2];


            try {
                task.execute();
            } catch (Exception exception) {
                printUsageAndExit(getStackTrace(exception));
            }
        }
    }

    private static boolean taskExists(String taskName) {
        for (AbstractFlinkTask task : tasks) {
            if (taskName.equals(task.getClass().getName())) {
                return true;
            }
        }

        return false;
    }

    private static AbstractFlinkTask getTaskFromArgs(String taskName) {
        for (AbstractFlinkTask task : tasks) {
            if (taskName.equals(task.getClass().getName())) {
                return task;
            }
        }

        return tasks.get(0);
    }

    private static void printUsageAndExit(String errorMessage) {
        System.out.println("\nError: " + errorMessage + "\n");

        System.out.println(
                "Usage: '/root/flink/bin/flink run FILENAME.jar PARALLELISM TASK OUTPUTPATH\n" +
                        "\nPARALLELISM:" +
                        "\n\t number of parallelism" +
                        "\n\t example: 3"
        );

        System.out.println("\nTASK:");

        for (AbstractFlinkTask task : tasks) {
            System.out.println("\t " + task.getTaskExecutionDescription());
        }

        System.out.println("\nOUTPUTPATH:\n\t example: hdfs:///example.txt");

        System.exit(0);
    }

    private static String getStackTrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }
}
