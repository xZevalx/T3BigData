/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.q2CustomerBillAvg;

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

public class DataQ2ToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(DataQ2ToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long nPurchases = 1;
        String client = tuple.getStringByField("client");
        double totalSpent = tuple.getDoubleByField("totalSpent");
        String table = "spent_per_client";

        System.out.println("Spent by client " + client + ": " + totalSpent);

        ResultSet results = session.execute("SELECT * FROM " + table + " WHERE client='"+client+"'");
        for (Row row : results) {
            totalSpent += row.getDouble("total_spent");
            nPurchases += row.getLong("n_purchases");
        }

        Statement statement = QueryBuilder.insertInto(table)
                .value("client", client)
                .value("n_purchases", nPurchases)
                .value("total_spent", totalSpent);
        LOG.debug(statement.toString());
        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
