package com.schwartech.accumulo;

import com.schwartech.pool.AccumuloConnectorFactory;
import com.schwartech.pool.AccumuloConnectorUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.pool2.impl.GenericObjectPool;
import play.Application;
import play.Configuration;
import play.Play;

import java.util.concurrent.TimeUnit;

/**
 * Created by jeff on 3/24/14.
 */
public class Accumulo {

    private static Config config;
    private static AccumuloConnectorUtil connectionPool;

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
        return connectionPool.getConnector();
    }

    public static void releaseConnector(Connector c) {
        connectionPool.releaseConnector(c);
    }

    public static int getNumThreads() {
        return config.numThreads;
    }

    public static BatchWriterConfig getDefaultWriterConfig() {
        return config.getBatchWriterConfig();
    }

    public static void initialize(Config config) {
        Accumulo.config = config;

        AccumuloConnectorFactory accumuloConnectorFactory = new AccumuloConnectorFactory()
                .username(config.username)
                .password(config.password)
                .instanceName(config.instanceName)
                .zooServers(config.zooServers);

        connectionPool = new AccumuloConnectorUtil(new GenericObjectPool<Connector>(accumuloConnectorFactory));
    }

}