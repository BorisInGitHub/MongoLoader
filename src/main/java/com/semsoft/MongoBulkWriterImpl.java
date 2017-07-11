package com.semsoft;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.apache.http.client.utils.URIBuilder;
import org.bson.Document;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * MongoWriterImpl
 * Created by breynard on 28/04/17.
 */
public class MongoBulkWriterImpl implements MongoWriter<MongoCollection<Document>> {

    private final MongoClient mongoClient;
    private final BulkWriteOptions bulkWriteOptions;

    public MongoBulkWriterImpl(String mongoURI) {
        super();
        mongoClient = buildMongoClient(mongoURI);
        bulkWriteOptions = getBulkWriteOptions();
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

    private static BulkWriteOptions getBulkWriteOptions() {
        // Attention, le bypassDocumentValidation avec le writeConcern nécessite un Mongo 3.4 à priori ...
        return new BulkWriteOptions()/*.bypassDocumentValidation(true)*/.ordered(false);
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

    @Override
    public void close() {
        mongoClient.close();
    }

    @Override
    public MongoCollection<Document> createCollection() {
//        boolean databaseAlreadyExist = false;
//        MongoIterable<String> databaseNames = mongoClient.listDatabaseNames();
//        for (String databaseName : databaseNames) {
//            if (databaseName.equals(DATABASE_NAME)) {
//                databaseAlreadyExist = true;
//                break;
//            }
//        }
//        if (databaseAlreadyExist) {
//            mongoClient.dropDatabase(DATABASE_NAME);
//        }
        MongoDatabase testDatabase = mongoClient.getDatabase(DATABASE_NAME);

        MongoCollection<Document> collection = testDatabase.getCollection(COLLECTION_NAME);
        if (collection != null) {
            collection.drop();
        }
        testDatabase.createCollection(COLLECTION_NAME,
                new CreateCollectionOptions()
                        .autoIndex(false)
                        .validationOptions(new ValidationOptions().validationLevel(ValidationLevel.OFF)));

        collection = testDatabase.getCollection(COLLECTION_NAME);

        // Ajout des données
        collection = collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED);

        return collection;
    }

    @Override
    public void createIndexs(MongoCollection<Document> collection, DataProcessor dataProcessor) {
        for (String columnName : dataProcessor.columsWithIndex()) {
            // Ajout des indexs (sur la colonne 0 par défaut), on pourrait en rajouer d'autres d'ailleurs
            collection.createIndex(
                    new BasicDBObject(columnName, 1),
                    new IndexOptions().unique(false));
        }
    }

    @Override
    public void writeRowsToMongo(MongoCollection<Document> collection, List<List<String>> rows, DataProcessor dataProcessor) {
        List<WriteModel<Document>> bulkWrite = new ArrayList<>(rows.size());
        for (List<String> row : rows) {
            Document document = convertRowToMongoDocument(dataProcessor.keepUsedColumns(row));
            bulkWrite.add(new InsertOneModel<>(document));
        }

        collection.bulkWrite(bulkWrite, bulkWriteOptions);
    }
}
