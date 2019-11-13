/**
 * Copyright 2016-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.examples;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *
 */
public class TruckEventsCsvConverter {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Program should have at least two arguments like below");
            System.out.println(" <truck-events-csv-payload-file-path> <output-file-path>");
            System.exit(1);
        }

        String payloadFilePath = args[0];
        String outputFilePath = args[1];
        TruckEventsCsvConverter truckEventsCsvConverter = new TruckEventsCsvConverter();
        truckEventsCsvConverter.convertToJsonRecords(payloadFilePath, outputFilePath);
    }

    public List<String> convertToJsonRecords(InputStream payloadStream, int limit) throws Exception {
        MappingIterator<TruckEvent> csvTruckEvents = readTruckEventsFromCsv(payloadStream);
        List<String> jsons = new ArrayList<>();
        int ct = 0;
        for (TruckEvent truckEvent : csvTruckEvents.readAll()) {
            truckEvent.setMiles((long) new Random().nextInt(500));
            String json = new ObjectMapper().writeValueAsString(truckEvent);
            jsons.add(json);
            if (++ct == limit) {
                break;
            }
        }

        return jsons;
    }

    public void convertToJsonRecords(String payloadFile, String outputFileName) throws IOException {
        try (InputStream csvStream = new FileInputStream(payloadFile);
             FileWriter fos = new FileWriter(outputFileName)) {
            MappingIterator<TruckEvent> csvTruckEvents = readTruckEventsFromCsv(csvStream);
            for (TruckEvent truckEvent : csvTruckEvents.readAll()) {
                truckEvent.setMiles((long) new Random().nextInt(500));
                String output = new ObjectMapper().writeValueAsString(truckEvent);
                fos.write(output);
                fos.write(System.lineSeparator());
            }
        }
    }

    private MappingIterator<TruckEvent> readTruckEventsFromCsv(InputStream csvStream) throws IOException {
        CsvSchema bootstrap = CsvSchema.builder()
// driverId,truckId,eventTime,eventType,longitude,latitude,eventKey,correlationId,driverName,routeId,routeName,eventDate
                .addColumn("driverId", CsvSchema.ColumnType.NUMBER)
                .addColumn("truckId", CsvSchema.ColumnType.NUMBER)
                .addColumn("eventTime", CsvSchema.ColumnType.STRING)
                .addColumn("eventType", CsvSchema.ColumnType.STRING)
                .addColumn("longitude", CsvSchema.ColumnType.NUMBER)
                .addColumn("latitude", CsvSchema.ColumnType.NUMBER)
                .addColumn("eventKey", CsvSchema.ColumnType.STRING)
                .addColumn("correlationId", CsvSchema.ColumnType.NUMBER)
                .addColumn("driverName", CsvSchema.ColumnType.STRING)
                .addColumn("routeId", CsvSchema.ColumnType.NUMBER)
                .addColumn("routeName", CsvSchema.ColumnType.STRING)
                .addColumn("eventDate", CsvSchema.ColumnType.STRING)
//                .addColumn("miles", CsvSchema.ColumnType.NUMBER)
                .build().withHeader();

        CsvMapper csvMapper = new CsvMapper();
        return csvMapper.readerFor(TruckEvent.class).with(bootstrap).readValues(csvStream);
    }

}
