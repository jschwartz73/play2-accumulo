package com.schwartech.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.security.Authorizations;
import play.Application;
import play.Play;

/**
 * Created by jeff on 3/24/14.
 */
public class Accumulo {
    //TODO: JSS - this should be a config property
    private static int DEFAULT_THREADS = 5;

    private static AccumuloPlugin getPlugin() {
        Application app = Play.application();
        if(app == null) {
            throw new RuntimeException("No application running");
        }

        AccumuloPlugin plugin = app.plugin(AccumuloPlugin.class);
        if(plugin == null) {
            throw new RuntimeException("AccumuloPlugin not found");
        }
        return plugin;
    }

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        AccumuloPlugin plugin = getPlugin();
        return plugin.getConnector();
    }

    public static Connector getConnector(String username, String password) throws AccumuloSecurityException, AccumuloException {
        AccumuloPlugin plugin = getPlugin();
        return plugin.getConnector(username, password);
    }

    public static BatchDeleter createBatchDeleter(String table, Authorizations auths) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        AccumuloPlugin plugin = getPlugin();
        return getConnector().createBatchDeleter(table, auths, DEFAULT_THREADS, plugin.getDefaultWriterConfig());
    }

    public static BatchWriter createBatchWriter(String table) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        AccumuloPlugin plugin = getPlugin();
        return getConnector().createBatchWriter(table, plugin.getDefaultWriterConfig());
    }

    public static BatchScanner createBatchScanner(String table, Authorizations auths) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        return getConnector().createBatchScanner(table, auths, DEFAULT_THREADS);
    }

    public static BatchScanner createBatchScanner(String table, Authorizations auths, int numThreads) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        return getConnector().createBatchScanner(table, auths, numThreads);
    }

    public static Scanner createScanner(String table, Authorizations auths) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        return getConnector().createScanner(table, auths);
    }
}