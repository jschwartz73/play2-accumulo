package com.schwartech.accumulo;

import com.schwartech.pool.AccumuloConnectorFactory;
import com.schwartech.pool.AccumuloConnectorUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.pool2.impl.GenericObjectPool;
import play.Application;
import play.Configuration;
import play.Logger;
import play.Plugin;

import java.util.concurrent.TimeUnit;

/**
 * Created by jeff on 3/24/14.
 */
/**
 * A Play plugin that automatically manages Accumulo configuration.
 */
public class AccumuloPlugin extends Plugin {

    private final Application application;
    public String username;
    private String password;
    private long memBuf;
    private long timeout;
    private int numThreads;
    private String instanceName;
    private String zooServers;
    private Boolean enabled;
    private BatchWriterConfig batchWriterConfig;

    private AccumuloConnectorUtil connectionPool;

    public AccumuloPlugin(Application application) {
        this.application = application;
    }

    /**
     * Reads the configuration file and initializes accumulo settings.
     */
    public void onStart() {

        Configuration accumuloConf = Configuration.root().getConfig("accumulo");

        if(accumuloConf == null) {
            Logger.info("Accumulo settings not found.");
        } else {
            Logger.info("Accumulo settings found:");

            enabled = accumuloConf.getBoolean("plugin.enabled", true);
            Logger.info(" * plugin.enabled: " + enabled);

            if (enabled) {
                username = accumuloConf.getString("username", "root");
                password = accumuloConf.getString("password", "secret");

                memBuf = accumuloConf.getLong("memBuf", 1000000L);
                timeout = accumuloConf.getLong("timeout", 1000L);
                numThreads = accumuloConf.getInt("numThreads", 10);

                instanceName = accumuloConf.getString("instanceName", "mock-instance");
                zooServers = accumuloConf.getString("zooServers", "localhost:2181");

                batchWriterConfig = new BatchWriterConfig()
                        .setMaxMemory(memBuf)
                        .setMaxWriteThreads(numThreads)
                        .setTimeout(timeout, TimeUnit.MILLISECONDS);
                Logger.info("Accumulo settings found: ");
                Logger.info(" * username: " + username);
                Logger.info(" * memBuf: " + memBuf);
                Logger.info(" * timeout: " + timeout);
                Logger.info(" * numThreads: " + numThreads);
                Logger.info(" * instanceName: " + instanceName);
                Logger.info(" * zooServers: " + zooServers);

                AccumuloConnectorFactory accumuloConnectorFactory = new AccumuloConnectorFactory()
                        .username(username)
                        .password(password)
                        .instanceName(instanceName)
                        .zooServers(zooServers);

                connectionPool = new AccumuloConnectorUtil(new GenericObjectPool<Connector>(accumuloConnectorFactory));
            }
        }
    }

    public Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return connectionPool.getConnector();
    }

    public void releaseConnector(Connector c) {
        connectionPool.releaseConnector(c);
    }

    public int getNumThreads() {
        return numThreads;
    }

    public BatchWriterConfig getDefaultWriterConfig() {
        return batchWriterConfig;
    }

}
