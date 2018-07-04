/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.q6SalesPerRegion;

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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;

public class FilterBoltQ6 extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FilterBoltQ6.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println("En filterbolt 1");
        try {
            System.out.println("En filterbolt 2");
            JsonNode object = new ObjectMapper().readTree(tuple.getString(0));

            JsonNode employee = object.get("empleado");
            JsonNode subsidiary = employee.get("sucursal");
            JsonNode addressComponents = subsidiary.get("address_components");

            String region = "";
            // Encontrar region
            for (JsonNode component : addressComponents) {
                if (component.get("types").get(0).asText().equals("administrative_area_level_1")) {
                    region = component.get("short_name").asText();
                    break;
                }
            }

            JsonNode products = object.get("productos");

            // Monto total de la boleta
            long total = 0;

            for (JsonNode product : products) {
                try {
                    total += product.get("salePrice").asLong();
                } catch (Exception e) {
                }
            }

            String date = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                    .format(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy/M/d H:m:s")
                                    .parse(object.get("fecha").asText())));

            System.out.println("Emitiendo " + region + ", " + total + ", " + date);

            collector.emit(new Values(region, total, date));

        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(FilterBoltQ6.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("region", "total", "date"));
    }
}
