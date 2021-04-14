import core.AbstractFlinkTask;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.core.fs.FileSystem;

class GrepTask extends AbstractFlinkTask {

    private String searchString;

    GrepTask(ExecutionEnvironment executionEnvironment) {
        super(executionEnvironment);
    }

    void setSearchString(String searchString) {
        this.searchString = searchString;
    }

    @Override
    public String getTaskExecutionDescription() {
        return super.getTaskExecutionDescription() + " SEARCHSTRING";
    }

    public void execute() throws IllegalArgumentException {
        if (searchString.isEmpty()) {
            throw new IllegalArgumentException("The search string is empty.");
        }

        DataSet<String> dsTextLines = executionEnvironment.readTextFile("hdfs:///twitter/twitter_rv.net");
        ReduceOperator<Integer> occurrence = dsTextLines.map(new StringContainsMapper(searchString)).reduce(new SumReducer());

        if (outputPath.isEmpty()) {
            throw new IllegalArgumentException("The outpath is empty.");
        }

        //write result to file
        //example 'hdfs:///twitter/grep_task.net'
        occurrence.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        executeEnvironment();
    }

    private class StringContainsMapper implements MapFunction<String, Integer> {
        private String searchString;

        StringContainsMapper(String searchString) {
            this.searchString = searchString;
        }

        @Override
        public Integer map(String line) throws Exception {
            return StringUtils.countMatches(line, searchString);
        }
    }

    private class SumReducer implements ReduceFunction<Integer> {
        @Override
        public Integer reduce(Integer number1, Integer number2) throws Exception {
            return number1 + number2;
        }
    }
}
