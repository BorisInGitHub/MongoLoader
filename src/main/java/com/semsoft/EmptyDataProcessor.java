package com.semsoft;

import java.util.Arrays;
import java.util.List;

/**
 * Created by breynard on 09/05/17.
 */
public class EmptyDataProcessor implements DataProcessor {
    @Override
    public List<String> keepUsedColumns(List<String> row) {
        return row;
    }

    @Override
    public List<String> columsWithIndex() {
        return Arrays.asList("0");
    }
}
