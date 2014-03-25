package com.schwartech.accumulo;

import com.schwartech.accumulo.operations.TableOperationsHelper;
import com.schwartech.accumulo.operations.UserOperationsHelper;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import play.Application;
import play.Play;

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
}
