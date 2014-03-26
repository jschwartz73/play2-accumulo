package com.schwartech.accumulo.operations;

import com.schwartech.accumulo.AccumuloPlugin;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import play.Logger;

import java.util.SortedSet;

/**
 * Created by jeff on 3/24/14.
 */
public class TableOperationsHelper {
    private AccumuloPlugin plugin;

    public TableOperationsHelper(AccumuloPlugin plugin) {
        this.plugin = plugin;
    }

    public boolean tableExists(String table) throws AccumuloSecurityException, AccumuloException {
        boolean exists = plugin.getConnector().tableOperations().exists(table);
        Logger.debug("Accumulo." + table + ", exists: " + exists);
        return exists;
    }

    public boolean deleteTable(String table) throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        boolean exists = tableExists(table);
        if (exists) {
            plugin.getConnector().tableOperations().delete(table);
        }
        exists = tableExists(table);
        return exists;
    }

    public boolean createTable(String table) throws AccumuloSecurityException, AccumuloException, TableExistsException {
        boolean exists = tableExists(table);

        if (!exists) {
            plugin.getConnector().tableOperations().create(table);
        }
        return exists = tableExists(table);
    }

    public SortedSet<String> getUserTables() throws Exception {
        SortedSet<String> tables = getAllTables();
        tables.remove("trace");
        tables.remove("!METADATA");
        return tables;
    }

    public SortedSet<String> getAllTables() throws AccumuloSecurityException, AccumuloException {
        SortedSet<String> tables = plugin.getConnector().tableOperations().list();
        return tables;
    }
}