package cl.uchile.tarea3.q2CustomerBillAvg;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class performQuery {

    public static void main(String[] args) {
        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("mari");

        ResultSet results = session.execute("SELECT * FROM spent_per_client");
        for (Row row : results) {
            System.out.println("Gasto promedio del cliente " + row.getString(0) + " es: " + row.getDouble(2)/row.getLong(1));
        }
        cluster.close();
    }
}
