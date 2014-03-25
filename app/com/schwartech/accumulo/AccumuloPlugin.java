package com.schwartech.accumulo;

import com.schwartech.accumulo.operations.*;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import play.Application;
import play.Configuration;
import play.Logger;
import play.Plugin;

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

    public UserOperationsHelper userOperationsHelper;
    public TableOperationsHelper tableOperationsHelper;

    private MockInstance mockInstance;

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
            username = accumuloConf.getString("username", "default");
            password = accumuloConf.getString("password", "secret");

            memBuf = accumuloConf.getLong("memBuf", 1000000L);
            timeout =accumuloConf.getLong("timeout", 1000L);
            numThreads = accumuloConf.getInt("numThreads", 10);

            instanceName = accumuloConf.getString("instanceName", "instance");
            zooServers = accumuloConf.getString("zooServers", "affy-master");

            userOperationsHelper = new UserOperationsHelper(this);
            tableOperationsHelper = new TableOperationsHelper(this);

            Logger.info("Accumulo settings found.  Username: " + username);
        }
    }

    public Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return getConnector(username, password);
    }

    public Connector getConnector(String username, String password) throws AccumuloSecurityException, AccumuloException {
        Instance inst = getZooKeeper();
        long t1 = System.currentTimeMillis();

//        return inst.getConnector(username, password);

        PasswordToken token = new PasswordToken(password.getBytes());
        return inst.getConnector(username, token);
    }

    private Instance getZooKeeper() {
        Instance instance;
        if (instanceName.contains("mock")) {
            if (mockInstance == null) {
                mockInstance = new MockInstance();
            }
            instance = mockInstance;
        } else {
            instance = new ZooKeeperInstance(instanceName, zooServers);
        }

        return instance;
    }
}
