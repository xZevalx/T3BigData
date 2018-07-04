/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.q6SalesPerRegion;

import cl.uchile.tarea3.CassandraBaseBolt;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataQ6ToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(DataQ6ToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long nSales = 1;
        String region = tuple.getStringByField("region");
        String date = tuple.getStringByField("date");
        long total = tuple.getLongByField("total");
        String table = "sales_per_region";

        ResultSet results = session.execute(
                "SELECT * FROM " + table + " WHERE region='"+region+"' and date='"+date+"'");

        for (Row row : results){
            total += row.getLong("total");
            nSales += row.getLong("n_sales");
        }

        String[] d = date.split("-");

        Statement statement = QueryBuilder.insertInto(table)
                .value("region", region)
                .value("date", LocalDate.fromYearMonthDay(Integer.parseInt(d[0]), Integer.parseInt(d[1]), Integer.parseInt(d[2])))
                .value("n_sales", nSales)
                .value("total", total);

        LOG.debug(statement.toString());
        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
