/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.q3TopTenProducts;

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

public class DataQ3ToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(DataQ3ToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long nSales = 1;
        String productName = tuple.getStringByField("productName");
        String productId = tuple.getStringByField("productId");
        String table = "sales_per_product";

        System.out.println("Product" + productName + "with ID " + productId);

        ResultSet results = session.execute("SELECT * FROM " + table + " WHERE product_id='"+productId+"'");
        for (Row row : results) {
            nSales += row.getLong("n_sales");
        }

        Statement statement = QueryBuilder.insertInto(table)
                .value("product_id", productId)
                .value("name", productName)
                .value("n_sales", nSales);
        LOG.debug(statement.toString());
        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
