package com.schwartech.accumulo.model;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by jeff on 3/20/14.
 */
public class DocumentIndexResultSet {
    public Map<Text, DocumentIndex> rowDocuments;

    public DocumentIndexResultSet() {
        rowDocuments = new HashMap<Text, DocumentIndex>();
    }

    public void add(Key key, Value value) {
        DocumentIndex d = getByRow(key.getRow());

        if (d == null) {
            d = new DocumentIndex();
        }

        d.add(key, value);

        rowDocuments.put(d.rowKey, d);
    }

    private DocumentIndex getByRow(Text key) {
        return rowDocuments.get(key);
    }

    public DocumentIndex get(String key) {
        return getByRow(new Text(key));
    }

    public Set<Range> getColQualifiers(String colFamily) {
        Set<Range> ranges = new HashSet<Range>();

        for (Text t : rowDocuments.keySet()) {
            ranges.addAll(rowDocuments.get(t).getColQualifiersAsRangeList(colFamily));
        }

        return ranges;
    }
}