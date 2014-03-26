package com.schwartech.accumulo.operations;

import com.schwartech.accumulo.AccumuloPlugin;
import com.schwartech.accumulo.model.DocumentIndexResultSet;
import com.schwartech.accumulo.model.DocumentResultSet;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import play.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by jeff on 3/24/14.
 */
public class ScannerOperationsHelper {
    private AccumuloPlugin plugin;

    public ScannerOperationsHelper(AccumuloPlugin plugin) {
        this.plugin = plugin;
    }

    public DocumentResultSet query(String table, Authorizations auths, Set<Range> ranges, Map<String, String> columnsToFetch) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        DocumentResultSet drs = new DocumentResultSet();

        BatchScanner scanner = plugin.createBatchScanner(table, auths);

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

    public DocumentIndexResultSet queryIndex(String table, Authorizations auths, Range range) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        DocumentIndexResultSet dirs = new DocumentIndexResultSet();

        Scanner indexScanner = plugin.createScanner(table, auths);

        indexScanner.setRange(range);

        for (Map.Entry<Key,Value> entry : indexScanner) {
            dirs.add(entry.getKey(), entry.getValue());
        }

        indexScanner.close();

        return dirs;
    }
}