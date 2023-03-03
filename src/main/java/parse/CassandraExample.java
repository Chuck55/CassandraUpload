package parse;

import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public class CassandraExample {
    private static String contactPoint = "127.0.0.1";
    private static int port = 9042;
    private static String keySpace = "cassandra_project";
    private static String dataCenter = "datastax-desktop";

    public static void main(String args[]) {

        try (CqlSession session = CqlSession.builder().addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(dataCenter).withKeyspace(keySpace).build()) {

            PreparedStatement prepared = session.prepare("insert into earliest_date (recorded_date) values ('04/05/1999')");
            BoundStatement bound = prepared.bind();
            session.execute(bound);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}