package com.semsoft;

import java.util.List;

/**
 * MongoWriter
 * Created by breynard on 28/04/17.
 */
public interface MongoWriter<Collection> {
    String DATABASE_NAME = "TEST";
    String COLLECTION_NAME = "Table1";

    Collection createCollection();

    void createIndexs(Collection collection, DataProcessor dataProcessor);

    void writeRowsToMongo(Collection collection, List<List<String>> rows, DataProcessor dataProcessor);

    void close();
}
