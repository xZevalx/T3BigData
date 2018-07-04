package cl.uchile.tarea3.q3TopTenProducts;

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

        ResultSet results = session.execute("SELECT * FROM sales_per_product");

        ArrayList<Row> r = new ArrayList<Row>();

        for (Row row : results) {
            r.add(row);
        }

        Collections.sort(r,
                new Comparator<Row>() {
            // Notar que lo queremos de mayor a menor
                    @Override
                    public int compare(Row o1, Row o2) {
                        long l1 = o1.getLong("n_sales");
                        long l2 = o2.getLong("n_sales");
                        if(l1 < l2 ) {
                            return 1;
                        }
                        else if (l1 == l2) {
                            return 0;
                        }
                        return -1;
                    }
                });

        System.out.println("Los diez productos más vendidos son:");
        for (int k = 0; k < 10; k++) {
            try {
                Row row = r.get(k);
                System.out.println(row.getString("name") + " con " + row.getLong("n_sales") + " ventas.");
            } catch (Exception e) {
                break;
            }
        }
        cluster.close();
    }


}
