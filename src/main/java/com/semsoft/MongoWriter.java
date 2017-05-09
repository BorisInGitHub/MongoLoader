package com.semsoft;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import javax.xml.crypto.Data;
import java.util.List;

/**
 * MongoWriter
 * Created by breynard on 28/04/17.
 */
public interface MongoWriter {
    String DATABASE_NAME = "TEST";
    String COLLECTION_NAME = "Table1";

    MongoCollection<Document> createCollection();

    void createIndexs(MongoCollection<Document> collection, DataProcessor dataProcessor);

    void writeRowsToMongo(MongoCollection<Document> collection, List<List<String>> rows, DataProcessor dataProcessor);
}
