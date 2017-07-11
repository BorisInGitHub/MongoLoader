package com.semsoft;

import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.async.client.MongoClients;
import com.mongodb.client.model.*;
import com.mongodb.connection.ClusterSettings;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * MongoAsyncWriter
 * Created by breynard on 11/07/17.
 */
public class MongoAsyncWriterImpl implements MongoWriter<com.mongodb.async.client.MongoCollection<Document>> {
    private final MongoClient mongoClient;
    private final InsertManyOptions insertManyOptions;


    public MongoAsyncWriterImpl(String mongoURI) {
        super();
        mongoClient = buildMongoClient(mongoURI);
        insertManyOptions = getInsertManyOptions();
    }

    private static InsertManyOptions getInsertManyOptions() {
        // Attention, le bypassDocumentValidation avec le writeConcern nécessite un Mongo 3.4 à priori ...
        return new InsertManyOptions()/*.bypassDocumentValidation(true)*/.ordered(false);
    }

    private static Document convertRowToMongoDocument(List<String> row) {
        Document document = new Document();
        int i = 0;
        for (String value : row) {
            document.put(Integer.toString(i), value);
            i++;
        }
        return document;
    }

    private MongoClient buildMongoClient(String mongoURI) {
//        ConnectionString connectionString = new ConnectionString(mongoURI);
        ClusterSettings clusterSettings = ClusterSettings.builder()
                .hosts(asList(new ServerAddress("localhost", 27017)))
                .build();

        return MongoClients.create(MongoClientSettings.builder()
                .clusterSettings(clusterSettings)
                .applicationName("Test Async")
                .writeConcern(WriteConcern.ACKNOWLEDGED)
                .build());
    }

    @Override
    public void close() {
        mongoClient.close();
    }

    @Override
    public com.mongodb.async.client.MongoCollection<Document> createCollection() {
        com.mongodb.async.client.MongoDatabase testDatabase = mongoClient.getDatabase(DATABASE_NAME);

        com.mongodb.async.client.MongoCollection<Document> collection = testDatabase.getCollection(COLLECTION_NAME);
        if (collection != null) {
            collection.drop(new SingleResultCallback<Void>() {
                @Override
                public void onResult(Void result, Throwable t) {

                }
            });
        }
        testDatabase.createCollection(COLLECTION_NAME,
                new CreateCollectionOptions()
                        .autoIndex(false)
                        .validationOptions(new ValidationOptions().validationLevel(ValidationLevel.OFF)), new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(Void result, Throwable t) {

                    }
                });

        collection = testDatabase.getCollection(COLLECTION_NAME);

        // Ajout des données
        collection = collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED);

        return collection;
    }

    @Override
    public void createIndexs(com.mongodb.async.client.MongoCollection<Document> collection, DataProcessor dataProcessor) {
        for (String columnName : dataProcessor.columsWithIndex()) {
            // Ajout des indexs (sur la colonne 0 par défaut), on pourrait en rajouer d'autres d'ailleurs
            collection.createIndex(
                    Indexes.hashed(columnName),
                    new IndexOptions().unique(false), new SingleResultCallback<String>() {
                        @Override
                        public void onResult(String result, Throwable t) {

                        }
                    });
        }
    }

    @Override
    public void writeRowsToMongo(com.mongodb.async.client.MongoCollection<Document> collection, List<List<String>> rows, DataProcessor dataProcessor) {
        List<Document> documents = new ArrayList<>(rows.size());
        for (List<String> row : rows) {
            Document document = convertRowToMongoDocument(dataProcessor.keepUsedColumns(row));
            documents.add(document);
        }

        collection.insertMany(documents, insertManyOptions, new SingleResultCallback<Void>() {
            @Override
            public void onResult(Void result, Throwable t) {

            }
        });
    }
}
