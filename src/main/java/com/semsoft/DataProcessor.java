package com.semsoft;

import java.util.List;

/**
 * Created by breynard on 09/05/17.
 */
public interface DataProcessor {
    List<String> keepUsedColumns(List<String> row);

    List<String> columsWithIndex();
}
