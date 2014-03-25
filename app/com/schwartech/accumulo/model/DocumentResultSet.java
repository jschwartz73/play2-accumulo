package com.schwartech.accumulo.model;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jeff on 3/20/14.
 */
public class DocumentResultSet {
    public Map<Text, Document> rowDocuments;

    public DocumentResultSet() {
        rowDocuments = new HashMap<Text, Document>();
    }

    public void add(Key key, Value value) {
        Document d = get(key.getRow());

        if (d == null) {
            d = new Document();
        }

        d.add(key, value);

        rowDocuments.put(d.rowKey, d);
    }

    public Document get(Text key) {
        return rowDocuments.get(key);
    }

    public Document get(String key) {
        return get(new Text(key));
    }
}