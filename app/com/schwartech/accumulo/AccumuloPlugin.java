package com.schwartech.accumulo;

import org.apache.accumulo.core.client.*;
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

    private Config config;

    private Boolean enabled;

    public AccumuloPlugin(Application application) {
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
                config.username = accumuloConf.getString("username", "root");
                config.password = accumuloConf.getString("password", "secret");

                config.memBuf = accumuloConf.getLong("memBuf", 1000000L);
                config.timeout = accumuloConf.getLong("timeout", 1000L);
                config.numThreads = accumuloConf.getInt("numThreads", 10);

                config.instanceName = accumuloConf.getString("instanceName", "mock-instance");
                config.zooServers = accumuloConf.getString("zooServers", "localhost:2181");

                Logger.info("Accumulo settings found: ");
                Logger.info(" * username: " + config.username);
                Logger.info(" * memBuf: " + config.memBuf);
                Logger.info(" * timeout: " + config.timeout);
                Logger.info(" * numThreads: " + config.numThreads);
                Logger.info(" * instanceName: " + config.instanceName);
                Logger.info(" * zooServers: " + config.zooServers);

            }
        }
    }

    @Override
    public boolean enabled() {
        Configuration accumuloConf = Configuration.root().getConfig("accumulo");

        enabled = false;
        if(accumuloConf != null) {
            enabled = accumuloConf.getBoolean("plugin.enabled", true);
        }

        return enabled;
    }
}
