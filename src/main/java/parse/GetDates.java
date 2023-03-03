package parse;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GetDates {
    public static void main(String[] args) throws IOException, CsvException {

        String fileName = "C:/Users/kylej/Desktop/ParsingDataProject/src/main/java/parse/RealEstateData.csv";
        try (CSVReader reader = new CSVReader(new FileReader(fileName))) {
            List<String[]> r = reader.readAll();
            getDates(r);
        }
    }

    public static void getDates(List<String[]> r) {
        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
        Date date = new Date();
        for (String[] x : r) {
            try {
                if (!Objects.equals(x[2], "")) {
                    Date newDate = format.parse(x[2]);
                    if (date.compareTo(newDate) > 0) {
                        date = newDate;
                    }
                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println(date);
    }
}

