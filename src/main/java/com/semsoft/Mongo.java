package com.semsoft;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.apache.http.client.utils.URIBuilder;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
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

            LOGGER.info("Chargement du fichier {} dans Mongo {}.", csvToRead, mongoURI);

            MongoClient mongoClient = buildMongoClient(mongoURI);

            String databaseName = "TEST";
            String collectionName = "Table1";
            MongoDatabase testDatabase = mongoClient.getDatabase(databaseName);
            mongoClient.dropDatabase(databaseName);
            testDatabase = mongoClient.getDatabase(databaseName);

            MongoCollection<Document> collection = testDatabase.getCollection(collectionName);
            if (collection == null) {
                testDatabase.createCollection(collectionName,
                        new CreateCollectionOptions()
                                .autoIndex(false)
                                .validationOptions(new ValidationOptions().validationLevel(ValidationLevel.OFF)));

                collection = testDatabase.getCollection(collectionName);
            }


            // Ajout des indexs (sur la colonne 0 par défaut), on pourrait en rajouer d'autres d'ailleurs
            collection.createIndex(
                    new BasicDBObject("0", 1),
                    new IndexOptions().unique(false));

            // Ajout des données
            collection = collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED);
            // Attention, le bypassDocumentValidation avec le writeConcern nécessite un Mongo 3.4 à priori ...
            BulkWriteOptions bulkWriteOptions = new BulkWriteOptions()/*.bypassDocumentValidation(true)*/.ordered(false);


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
                                    writeRowsToMongo(collection, bulkWriteOptions, rows);
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
                writeRowsToMongo(collection, bulkWriteOptions, rows);
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

    private static void writeRowsToMongo(MongoCollection<Document> collection, BulkWriteOptions bulkWriteOptions, List<List<String>> rows) {
        List<WriteModel<Document>> bulkWrite = new ArrayList<>(rows.size());
        for (List<String> row : rows) {
            Document document = convertRowToDocument(row);
            bulkWrite.add(new InsertOneModel<>(document));
        }

        collection.bulkWrite(bulkWrite, bulkWriteOptions);
    }

    private static Document convertRowToDocument(List<String> row) {
        Document document = new Document();
        int i = 0;
        for (String value : row) {
            document.put(Integer.toString(i), value);
            i++;
        }
        return document;
    }

    private static CsvPreference buildCsvPreference() {
        return CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE;
    }

    private static MongoClient buildMongoClient(String mongoURI) {
        MongoClientOptions.Builder options = MongoClientOptions.builder()
                .applicationName("AggregoServer")
                .writeConcern(WriteConcern.ACKNOWLEDGED);
        // Put the write concern
        URI connectionURI;
        try {
            connectionURI = new URIBuilder(mongoURI).addParameter("w", "1").build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return new MongoClient(new MongoClientURI(connectionURI.toString(), options));
    }
}
