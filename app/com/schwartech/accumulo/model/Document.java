package com.schwartech.accumulo.model;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by jeff on 3/20/14.
 */
public class Document {
    public Text rowKey = null;

    // TODO: JSS - I'm not sure if we need the original data
//    private Map<Key, Value> originalData;
    private Map<String, Value> indexData;

    public Document() {
//        originalData = new HashMap<Key, Value>();
        indexData = new HashMap<String, Value>();
    }

    public void add(Key key, Value value) throws IllegalArgumentException {
        if (rowKey == null) {
            rowKey = key.getRow();
        } else if (!key.getRow().equals(rowKey)) {
            throw new IllegalArgumentException("Cannot add this data.  Row keys are different: " + rowKey.toString() + "/" + key.getRow().toString());
        }

//        originalData.put(key, value);
        indexData.put(getIndexKey(key.getColumnFamily(), key.getColumnQualifier()), value);
    }

    private String getIndexKey(String colFamily, String colQualifier) {
        return colFamily + ":" + colQualifier;
    }

    private String getIndexKey(Text colFamily, Text colQualifier) {
        return colFamily + ":" + colQualifier;
    }

    public Value getValue(String colFamily, String colQualifier) {
        Value value = indexData.get(getIndexKey(colFamily, colQualifier));

        //TODO: JSS - this should return null as that is different than ""
        if (value == null) {
            value = new Value("".getBytes());
        }
        return value;
    }

    public Set<String> getKeys() {
        return indexData.keySet();
    }

    public String getValueAsString(String colFamilyQualifier) {
        int idx = colFamilyQualifier.indexOf(':');
        String fam = colFamilyQualifier.substring(0, idx);
        String qual = colFamilyQualifier.substring(idx+1);
        return getValueAsString(fam, qual);
    }

    public String getValueAsString(String colFamily, String colQualifier) {
        return getValue(colFamily, colQualifier).toString();
    }
}
