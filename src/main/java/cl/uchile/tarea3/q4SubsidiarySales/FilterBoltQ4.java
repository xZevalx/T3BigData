/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.q4SubsidiarySales;

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

public class FilterBoltQ4 extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FilterBoltQ4.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            JsonNode object = new ObjectMapper().readTree(tuple.getString(0));
            JsonNode employee = object.get("empleado");
            JsonNode subsidiary = employee.get("sucursal");
            int subsidiaryId = subsidiary.get("id").asInt();
            String location = subsidiary.get("formatted_address").asText();

            collector.emit(new Values(subsidiaryId, location));

        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(FilterBoltQ4.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subsidiaryId", "location"));
    }
}
