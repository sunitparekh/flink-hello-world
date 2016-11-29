import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.ResultSet;

public class MovieJdbcAnalysis {

    public static final TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
    };

    public static final RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        InputFormat jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("org.h2.Driver")
                .setDBUrl("jdbc:h2:tcp://localhost/~/test")
                .setUsername("sa")
                .setQuery("select * from movies")
                .setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        DataStreamSource<Row> movies = env.createInput(jdbcInput);

        movies.filter(m -> ((Integer) m.productElement(0)) < 100).print();

        env.execute();
    }
}
