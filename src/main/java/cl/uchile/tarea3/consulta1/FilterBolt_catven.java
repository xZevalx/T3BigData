/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.consulta1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

/**
 *
 * @author FelipeEsteban
 */
public class FilterBolt_catven extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FilterBolt_catven.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            //mapeamos el objeto
            JsonNode object = new ObjectMapper().readTree(tuple.getString(0));

            JsonNode productos = object.get("productos");
            
          //obtenemos fecha como string
            String fecha; 
            
            DateTimeFormatter a;
            LocalDateTime b;
            DateTimeFormatter c;
            
            int category_id;
            if (productos.isArray()) {
            	//itereamos sobre los productos de la boleta
                for (JsonNode producto : productos) {
                    try {
                        category_id=producto.get("category_id").asInt();
                        
                        UUID idOne = UUID.randomUUID();
           
                      //Emitimos los valores
                        System.out.println("\n \n");
                        System.out.println(category_id);
                        System.out.println(idOne);
                        System.out.println("\n \n");
                        collector.emit(new Values(category_id, idOne));
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                }
 
            }

        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(FilterBolt_catven.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //nombre que tendran los valores
        declarer.declare(new Fields("categoria_id", "id"));
    }
}
