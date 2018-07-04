package cl.uchile.tarea3.q5SalesmanMostSales;

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

        // Necesitamos el valor máximo. Esto en cassandra solo devuelve una fila cuando pueden existir múltiples filas con ese valor
        ResultSet results = session.execute("SELECT max(n_sales) as n_sales FROM salesman_sales ALLOW FILTERING");

        long maxSales = results.one().getLong("n_sales");

        ResultSet results2 = session.execute("SELECT * FROM salesman_sales where n_sales="+maxSales+" ALLOW FILTERING");

        System.out.println("Las empleados con más ventas son:");
        for (Row row : results2) {
            System.out.println("Rut: " + row.getString("salesman") + " con " + row.getLong("n_sales")+ " ventas.");
        }

        cluster.close();
    }


}
