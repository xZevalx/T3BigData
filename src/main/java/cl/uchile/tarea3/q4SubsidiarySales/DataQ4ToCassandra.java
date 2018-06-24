/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.q4SubsidiarySales;

import cl.uchile.tarea3.CassandraBaseBolt;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataQ4ToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(DataQ4ToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long nSales = 1;
        int subsidiaryId = tuple.getIntegerByField("subsidiaryId");
        String location = tuple.getStringByField("location");
        String table = "subsidiary_sales";

        System.out.println("Subsidiary with id" + subsidiaryId + "located at " + location);

        ResultSet results = session.execute("SELECT * FROM " + table + " WHERE subsidiary_id='"+subsidiaryId+"'");
        for (Row row : results) {
            nSales += row.getLong("n_sales");
        }

        Statement statement = QueryBuilder.insertInto(table)
                .value("subsidiary_id", subsidiaryId)
                .value("location", location)
                .value("n_sales", nSales);
        LOG.debug(statement.toString());
        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
