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

public class NonUseCodes {
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
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    public static void getNonUseCodes(List<String[]> r) {
        HashMap<Integer, Integer> nonUseCodes = new HashMap<Integer, Integer>();
        for (String[] x : r) {
            if (x[10].length() > 0) {
                String code = "";
                if (x[10].length() == 1) {
                    code = x[10].substring(0,1);
                } else {
                    code = x[10].substring(0,2);
                }
                if (isNumeric(code)) {
                    int intCode = Integer.parseInt(code);
                    nonUseCodes.merge(intCode, 1, Integer::sum);
                }
            } else {
                nonUseCodes.merge(0, 1, Integer::sum);
            }
        }
        printNonUseCodes(nonUseCodes);
    }

    public static void printNonUseCodes(HashMap<Integer, Integer> nonUseCodes) {
        System.out.println(nonUseCodes);
        Set keys = nonUseCodes.keySet();
        Iterator i = keys.iterator();
        while (i.hasNext()) {
            int x = (int) i.next();
            transfer(x, nonUseCodes.get(x));
            //System.out.println(x + ": " + nonUseCodes.get(x));
        }
    }

    public static void transfer(int key, int value) {
        try (CqlSession session = CqlSession.builder().addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(dataCenter).withKeyspace(keySpace).build()) {

            PreparedStatement prepared = session.prepare("update cassandra_project.nonUseCodes set non_use_cd_count = non_use_cd_count+" + value + " where non_use_cd_id = " + key);
            BoundStatement bound = prepared.bind();
            session.execute(bound);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
