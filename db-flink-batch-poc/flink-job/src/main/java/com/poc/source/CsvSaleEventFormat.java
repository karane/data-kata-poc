package com.poc.source;

import com.poc.model.SaleEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Reads a daily CSV file from RustFS and emits one SaleEvent per data row.
 * Expected format (header row is skipped):
 *   saleId,salesmanId,salesmanName,city,region,productId,amount,eventTime
 */
public class CsvSaleEventFormat extends SimpleStreamFormat<SaleEvent> {

    @Override
    public Reader<SaleEvent> createReader(Configuration config, FSDataInputStream stream)
            throws IOException {

        BufferedReader br = new BufferedReader(new InputStreamReader(stream));

        return new Reader<SaleEvent>() {

            private boolean headerSkipped = false;

            @Override
            public SaleEvent read() throws IOException {
                String line;
                while ((line = br.readLine()) != null) {
                    if (!headerSkipped) {
                        headerSkipped = true;
                        continue; // skip header
                    }
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    return parseLine(line);
                }
                return null; // end of file
            }

            @Override
            public void close() throws IOException {
                br.close();
            }
        };
    }

    @Override
    public TypeInformation<SaleEvent> getProducedType() {
        return TypeInformation.of(SaleEvent.class);
    }

    private static SaleEvent parseLine(String line) {
        String[] cols = line.split(",", -1);
        SaleEvent e = new SaleEvent();
        e.saleId       = cols[0].trim();
        e.salesmanId   = cols[1].trim();
        e.salesmanName = cols[2].trim();
        e.city         = cols[3].trim();
        e.region       = cols[4].trim();
        e.productId    = cols[5].trim();
        e.amount       = Double.parseDouble(cols[6].trim());
        e.eventTime    = Long.parseLong(cols[7].trim());
        e.source       = "rustfs";
        return e;
    }
}
