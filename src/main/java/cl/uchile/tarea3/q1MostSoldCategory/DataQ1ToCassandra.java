package cl.uchile.tarea3.q1MostSoldCategory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import cl.uchile.tarea3.CassandraBaseBolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataQ1ToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(DataQ1ToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long nSales = 1;
        long category = tuple.getLongByField("category");
        String table="sales_per_category";

        System.out.println("Actualizando categoria: " + category);

        ResultSet results = session.execute("SELECT * FROM " + table + " WHERE category="+category);
        for (Row row : results) {
            nSales += row.getLong("n_sales");
        }

        Statement statement = QueryBuilder.insertInto(table)
                .value("category", category)
                .value("n_sales", nSales);
        LOG.debug(statement.toString());
        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
