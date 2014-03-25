package com.schwartech.accumulo;

import com.schwartech.accumulo.operations.TableOperationsHelper;
import com.schwartech.accumulo.operations.UserOperationsHelper;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.security.Authorizations;
import play.Application;
import play.Play;

import java.util.concurrent.TimeUnit;

/**
 * Created by jeff on 3/24/14.
 */
public class Accumulo {
    private static AccumuloPlugin getPlugin() {
        Application app = Play.application();
        if(app == null) {
            throw new RuntimeException("No application running");
        }

        AccumuloPlugin accumuloPlugin = app.plugin(AccumuloPlugin.class);
        if(accumuloPlugin == null) {
            throw new RuntimeException("AccumuloPlugin not found");
        }
        return accumuloPlugin;
    }

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        AccumuloPlugin accumuloPlugin = getPlugin();
        return accumuloPlugin.getConnector();
    }

    public static Connector Connector(String username, String password) throws AccumuloSecurityException, AccumuloException {
        AccumuloPlugin accumuloPlugin = getPlugin();
        return accumuloPlugin.getConnector();
    }

    public static UserOperationsHelper getUserOperationsHelper() {
        AccumuloPlugin accumuloPlugin = getPlugin();
        return accumuloPlugin.userOperationsHelper;
    }

    public static TableOperationsHelper getTableOperationsHelper() {
        AccumuloPlugin accumuloPlugin = getPlugin();
        return accumuloPlugin.tableOperationsHelper;
    }

    public static BatchWriter createBatchWriter(String table) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        AccumuloPlugin accumuloPlugin = getPlugin();
        return accumuloPlugin.createBatchWriter(table);
    }

    public BatchScanner createBatchScanner(String table, Authorizations auths) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        AccumuloPlugin accumuloPlugin = getPlugin();
        return accumuloPlugin.createBatchScanner(table, auths);
    }

    public BatchScanner createBatchScanner(String table, Authorizations auths, int numThreads) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        AccumuloPlugin accumuloPlugin = getPlugin();
        return accumuloPlugin.createBatchScanner(table, auths, numThreads);
    }

    public Scanner createScanner(String table, Authorizations auths) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        AccumuloPlugin accumuloPlugin = getPlugin();
        return accumuloPlugin.createScanner(table, auths);
    }

}
