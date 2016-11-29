import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Row;

public class MovieCsvAnalysis {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, String, String>> movies = env.readCsvFile("file:///Users/sunit/Projects/learning-spark/movies-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .types(Integer.class, String.class, String.class);

        OutputFormat jdbcSink = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("org.h2.Driver")
                .setDBUrl("jdbc:h2:tcp://localhost/~/test")
                .setUsername("sa")
                .setQuery("insert into movies (movie_id,movie_name,genre) values (?,?,?)")
                .finish();

        DataSet<Row> rows = movies.map(new TupleToRowExtractor());

        rows.output(jdbcSink);

        env.execute();
    }


    public static class TupleToRowExtractor implements MapFunction<Tuple3<Integer,String, String>, Row> {
        @Override
        public Row map(Tuple3<Integer, String, String> tuple) throws Exception {
            Row row = new Row(3);
            row.setField(0,tuple.f0);
            row.setField(1,tuple.f1);
            row.setField(2,tuple.f2);
            return row;
        }
    }
}
