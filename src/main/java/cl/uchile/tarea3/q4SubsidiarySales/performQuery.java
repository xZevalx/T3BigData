package cl.uchile.tarea3.q4SubsidiarySales;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class performQuery {

    public static void main(String[] args) {
        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("mari");

        ResultSet results = session.execute("SELECT subsidiary_id, location, max(n_sales) as n_sales FROM subsidiary_sales ALLOW FILTERING");

        System.out.println("Las sucursales con más ventas son:");
        for (Row row : results) {
            System.out.println("ID: " + row.getLong("subsidiary_id") + " ubicada en " + row.getString("location") + " con " + row.getLong("n_sales")+ " ventas.");
        }

        cluster.close();
    }


}
