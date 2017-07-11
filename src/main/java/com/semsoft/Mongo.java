package com.semsoft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Mongo tester
 * Created by breynard on 28/04/17.
 */
public class Mongo {
    private static final Logger LOGGER = LoggerFactory.getLogger(Mongo.class);

    public static void main(String args[]) throws IOException {
        if (args.length >= 2) {
            String mongoURI = args[0];
            String csvToRead = args[1];

            String engineName = "bulk";
            if (args.length >= 3) {
                engineName = args[2];
            }

            LOGGER.info("Chargement du fichier {} dans Mongo {}.", csvToRead, mongoURI);

            MongoWriter mongoWriter;
            if (engineName.equalsIgnoreCase("bulk")) {
                mongoWriter = new MongoBulkWriterImpl(mongoURI);
            } else if (engineName.equalsIgnoreCase("many")) {
                mongoWriter = new MongoManyWriterImpl(mongoURI);
            } else if (engineName.equalsIgnoreCase("async")) {
                mongoWriter = new MongoAsyncWriterImpl(mongoURI);
            } else {
                System.out.println("Pas de moteur sélectionné");
                System.exit(-1);
                return;
            }
            LOGGER.info("Test avec le moteur {}", mongoWriter.getClass().getName());

            String dataProc = "empty";
            if (args.length >= 4) {
                dataProc = args[3];
            }
            DataProcessor dataProcessor;
            if (dataProc.equalsIgnoreCase("astria")) {
                dataProcessor = new AstriaDataProcessor();
            } else {
                dataProcessor = new EmptyDataProcessor();
            }
            LOGGER.info("Test avec le moteur {}", dataProcessor.getClass().getName());


            Object mongoCollection = mongoWriter.createCollection();
            mongoWriter.createIndexs(mongoCollection, dataProcessor);

            int nbLines = 0;
            long start = System.currentTimeMillis();
            // Lecture des données
            int capacity = 1000;
            List<List<String>> rows = new ArrayList<>(capacity);
            try (FileInputStream inputStream = new FileInputStream(csvToRead)) {
                try (InputStreamReader inputStreamReader = new InputStreamReader(new BufferedInputStream(inputStream), StandardCharsets.UTF_8)) {
                    try (CsvListReader csvListReader = new CsvListReader(inputStreamReader, buildCsvPreference())) {
                        List<String> read = csvListReader.read();
                        // La première ligne est les headers
                        if (read == null || read.size() <= 0) {
                            // Au moins un header et une colonne dans le fichier
                            throw new RuntimeException("No header or no column");
                        }
                        while (read != null) {
                            read = csvListReader.read();

                            if (read != null) {
                                if (rows.size() == capacity) {
                                    mongoWriter.writeRowsToMongo(mongoCollection, rows, dataProcessor);
                                    rows.clear();
                                }
                                rows.add(read);
                                nbLines++;
                            }
                        }
                    }
                }
            }
            if (!rows.isEmpty()) {
                mongoWriter.writeRowsToMongo(mongoCollection, rows, dataProcessor);
                rows.clear();
            }
            long duration = System.currentTimeMillis() - start;

            LOGGER.info("Pour insérer {} lignes on a mis {} ms.", nbLines, duration);
            System.exit(0);
        } else {
            System.out.println("Error on doit passer les paramètres mongoURI et csvToRead");
            System.exit(-1);
        }
    }


    private static CsvPreference buildCsvPreference() {
        return CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE;
    }
}
