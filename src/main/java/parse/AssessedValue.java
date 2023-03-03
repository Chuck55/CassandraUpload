package parse;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class AssessedValue {
    private static String contactPoint = "127.0.0.1";
    private static int port = 9042;
    private static String keySpace = "cassandra_project";
    private static String dataCenter = "datastax-desktop";

    public static void main(String[] args) throws IOException, CsvException {

        String fileName = "C:/Users/kylej/Desktop/ParsingDataProject/src/main/java/parse/RealEstateData.csv";
        try (CSVReader reader = new CSVReader(new FileReader(fileName))) {
            List<String[]> r = reader.readAll();
            getNonUseCodes(r);
        }
    }

    public static boolean isNumeric(String str) {
        if (str == null) {
            return false;
        }
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    public static void getNonUseCodes(List<String[]> r) {
        HashMap<Integer, Integer> nonUseCodes = new HashMap<Integer, Integer>();
        double maxValue = 0;
        String town = "";
        String address = "";
        for (String[] x : r) {
            if (isNumeric(x[5]) && Double.parseDouble(x[5]) > maxValue) {
                maxValue = Double.parseDouble(x[5]);
                town = x[3];
                address = x[4];
            }
        }
        System.out.println(town);
        System.out.println(address);
        transfer(maxValue, town, address);
    }

    public static void transfer(double maxValue, String town, String address) {
        try (CqlSession session = CqlSession.builder().addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(dataCenter).withKeyspace(keySpace).build()) {

            // PreparedStatement prepared = session.prepare("update cassandra_project.maxAssessedValue set non_use_cd_count = non_use_cd_count+" + value + " where non_use_cd_id = " + key);
            PreparedStatement prepared = session.prepare("insert into property_assessed_value_sale_count (town_std, address_std, assessed_val_max) values ('" + town + "','" + address + "',"+ maxValue + ")");
            BoundStatement bound = prepared.bind();
            session.execute(bound);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
