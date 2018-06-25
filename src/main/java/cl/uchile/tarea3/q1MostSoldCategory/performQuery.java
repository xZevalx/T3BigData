package cl.uchile.tarea3.q1MostSoldCategory;

import com.datastax.driver.core.*;

public class performQuery {

    public static void main(String[] args) {
        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("mari");

        ResultSet results = session.execute("SELECT category, max(n_sales) FROM sales_per_category");
        for (Row row : results) {
            System.out.println("Categoría con ID " + row.getLong(0) + " tiene el número de ventas " + row.getLong(1));
        }
        cluster.close();
    }
}
