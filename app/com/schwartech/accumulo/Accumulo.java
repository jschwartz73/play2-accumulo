package com.schwartech.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.security.Authorizations;
import play.Application;
import play.Configuration;
import play.Play;

/**
 * Created by jeff on 3/24/14.
 */
public class Accumulo {
    private static int confDefaultTheads = 10;

    private static AccumuloPlugin getPlugin() {
        Application app = Play.application();
        if(app == null) {
            throw new RuntimeException("No application running");
        }

        AccumuloPlugin plugin = app.plugin(AccumuloPlugin.class);
        if(plugin == null) {
            throw new RuntimeException("AccumuloPlugin not found");
        }

        confDefaultTheads = Configuration.root().getConfig("accumulo").getInt("numThreads", 10);

        return plugin;
    }

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        AccumuloPlugin plugin = getPlugin();
        return plugin.getConnector();
    }

    public static void closeConnector(Connector c) {
        AccumuloPlugin plugin = getPlugin();
        plugin.closeConnector(c);
    }

}