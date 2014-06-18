package com.schwartech.pool;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.concurrent.TimeUnit;

/**
 * Created by jeff on 6/17/14.
 */
public class AccumuloConnectorFactory extends BasePooledObjectFactory<Connector> {
    private String username;
    private AuthenticationToken token;
    private String instanceName;
    private String zooServers;

    private static Instance mockInstance = null;

    public AccumuloConnectorFactory() {
        this.username = "root";
        token = new PasswordToken("password");
        this.instanceName = "mock-instance";
        this.zooServers = "127.0.0.1:2181";
    }

    public AccumuloConnectorFactory username(String s) {
        this.username = s;
        return this;
    }

    public AccumuloConnectorFactory password(String s) {
        this.token = new PasswordToken(s);
        return this;
    }

    public AccumuloConnectorFactory instanceName(String s) {
        this.instanceName = s;
        return this;
    }

    public AccumuloConnectorFactory zooServers(String s) {
        this.zooServers = s;
        return this;
    }

    @Override
    public Connector create() throws Exception {
        Instance inst = getZooKeeper();
        return inst.getConnector(username, token);
    }

    @Override
    public PooledObject<Connector> wrap(Connector connector) {
        return new DefaultPooledObject<Connector>(connector);
    }

    /**
     * When an object is returned to the pool, clear the buffer.
     */
//    @Override
//    public void passivateObject(PooledObject<Connector> pooledObject) {
//        pooledObject.getObject().setLength(0);
//    }

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