package com.schwartech.accumulo.helpers;

import com.schwartech.accumulo.Accumulo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.util.SortedSet;

/**
 * Created by jeff on 3/24/14.
 */
public class TableHelper {

    public static boolean tableExists(String table) throws AccumuloSecurityException, AccumuloException {
        return Accumulo.getConnector().tableOperations().exists(table);
    }

    public void deleteTable(String table) throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Accumulo.getConnector().tableOperations().delete(table);
    }

    public static void createTable(String table) throws AccumuloSecurityException, AccumuloException, TableExistsException {
        Accumulo.getConnector().tableOperations().create(table);
    }

    public static SortedSet<String> getUserTables() throws AccumuloSecurityException, AccumuloException {
        SortedSet<String> tables = getAllTables();
        tables.remove("trace");
        tables.remove("!METADATA");
        return tables;
    }

    public static SortedSet<String> getAllTables() throws AccumuloSecurityException, AccumuloException {
        return Accumulo.getConnector().tableOperations().list();
    }
}