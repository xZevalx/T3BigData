/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.q2CustomerBillAvg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.logging.Level;

public class FilterBoltQ2 extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FilterBoltQ2.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            JsonNode object = new ObjectMapper().readTree(tuple.getString(0));
            JsonNode products = object.get("productos");
            String client = object.get("cliente").asText();

            if (client.equals("")) {
                client = null;
            }

            double totalSpent = 0;

            for (JsonNode product : products) {
                try {
                    totalSpent += product.get("salePrice").asDouble();
                } catch (Exception e) {
                    System.out.println(e);
                }
            }

            collector.emit(new Values(client, totalSpent));

        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(FilterBoltQ2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("client", "totalSpent"));
    }
}
