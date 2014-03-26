package com.schwartech.accumulo.operations;

import com.schwartech.accumulo.Accumulo;
import com.schwartech.accumulo.AccumuloPlugin;
import com.schwartech.accumulo.model.DocumentIndexResultSet;
import com.schwartech.accumulo.model.DocumentResultSet;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import play.Logger;

import java.util.*;

/**
 * Created by jeff on 3/24/14.
 */
public class DataOperations {

    public static DocumentResultSet query(String table, Authorizations auths, Set<Range> ranges, Map<String, String> columnsToFetch) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        DocumentResultSet drs = new DocumentResultSet();

        BatchScanner scanner = Accumulo.createBatchScanner(table, auths);

        if (!ranges.isEmpty()) {
            scanner.setRanges(ranges);
        }

        for (Map.Entry<String, String> entry : columnsToFetch.entrySet()) {
            scanner.fetchColumn(new Text(entry.getKey()), new Text(entry.getValue()));
        }

        for (Map.Entry<Key,Value> entry : scanner) {
            drs.add(entry.getKey(), entry.getValue());
        }

        scanner.close();

        return drs;
    }

    public static DocumentIndexResultSet queryIndex(String table, Authorizations auths, Range range) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        DocumentIndexResultSet dirs = new DocumentIndexResultSet();

        Scanner indexScanner = Accumulo.createScanner(table, auths);

        indexScanner.setRange(range);

        for (Map.Entry<Key,Value> entry : indexScanner) {
            dirs.add(entry.getKey(), entry.getValue());
        }

        indexScanner.close();

        return dirs;
    }

    public static void deleteRange(String table, Authorizations auths, Range range) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        BatchDeleter deleter = Accumulo.createBatchDeleter(table, auths);
        List<Range> ranges = new ArrayList<Range>();
        ranges.add(range);
        deleteRanges(table, auths, ranges);
    }

    public static void deleteRanges(String table, Authorizations auths, Collection<Range> ranges) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        BatchDeleter deleter = Accumulo.createBatchDeleter(table, auths);
        deleter.setRanges(ranges);
        deleter.delete();
        deleter.close();
    }
}