/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.consulta1;

import static cl.uchile.tarea3.Cassandra.getTimeFromUUID;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import cl.uchile.tarea3.CassandraBaseBolt;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author FelipeEsteban
 */
public class catvenToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(catvenToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long categoria_id = 0, total_categoria = 0;
        String tableName="catven";
        System.out.println("Categoria_id: " +tuple.getStringByField("categoria_id")+ tuple.getStringByField("id"));

        //Consultamos por el registro con la fecha solicitada
        ResultSet results = session.execute("SELECT * FROM "+tableName);
        for (Row row : results) {
            categoria_id = row.getLong("categoria_id");
            total_categoria = row.getLong("total_categoria");
        }

        String[] a = tuple.getStringByField("date").split("-");

        if ("null".equals(tuple.getStringByField("cliente"))) {
            //Sumamos solo al total si el usuario es null
            Statement statement = QueryBuilder.insertInto(tableName)
                    .value("date", LocalDate.fromYearMonthDay(Integer.parseInt(a[0]), Integer.parseInt(a[1]), Integer.parseInt(a[2])))
                    .value("categoria_id", categoria_id)
                    .value("total_categoria", total_categoria + 1);
            LOG.debug(statement.toString());
            session.execute(statement);
        } else {
            //Si el usuario es distinto de null, sumamos en ambos lados
            Statement statement = QueryBuilder.insertInto(tableName)
                    .value("date", LocalDate.fromYearMonthDay(Integer.parseInt(a[0]), Integer.parseInt(a[1]), Integer.parseInt(a[2])))
                    .value("categoria_id", categoria_id + 1)
                    .value("total_categoria", total_categoria + 1);
            LOG.debug(statement.toString());
            session.execute(statement);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
