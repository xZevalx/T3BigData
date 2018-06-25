/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.q5SalesmanMostSales;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class LocalStormQ5 {

    /**
     * @param args the command line arguments
     */
    private static final Logger LOG = LoggerFactory.getLogger(LocalStormQ5.class);

    public static void main(String[] args) {
        //Configuracion de Storm para que lea la cola Local de Kafka
        String BROKER_LIST = "localhost:9092";
        String KAFKA_TOPIC = "kafka_local";
        String KAFKA_CONSUMER_GROUP = "storm";
        String ZOOKEEPER_HOST = "localhost:2181";
        String ZOOKEEPER_ROOT = "/mari_spout";
        BrokerHosts hosts = new ZkHosts(ZOOKEEPER_HOST);
        SpoutConfig spoutConfig = new SpoutConfig(
                hosts,
                KAFKA_TOPIC,
                ZOOKEEPER_ROOT,
                KAFKA_CONSUMER_GROUP
        );
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        Config config = new Config();
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", KAFKA_CONSUMER_GROUP);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(KafkaBolt.TOPIC, props);
        //aqui termina la configuración

        //Creamos Topologia
        TopologyBuilder builder = new TopologyBuilder();
        
        //Accedemos al Spout de Kafka definido previamente
        builder.setSpout("KafkaSpout", kafkaSpout);

        builder.setBolt("SalesmanFilter", new FilterBoltQ5(), 1)
                .shuffleGrouping("KafkaSpout");

        builder.setBolt("subsidiarySales", new DataQ5ToCassandra(), 1)
                .shuffleGrouping("SalesmanFilter");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Tarea3", config, builder.createTopology());

    }

}
