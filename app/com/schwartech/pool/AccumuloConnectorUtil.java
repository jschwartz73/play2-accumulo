package com.schwartech.pool;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.commons.pool2.ObjectPool;

/**
 * Created by jeff on 6/17/14.
 */
public class AccumuloConnectorUtil {
    private ObjectPool<Connector> pool;

    public AccumuloConnectorUtil(ObjectPool<Connector> pool) {
        this.pool = pool;
    }

    public Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        Connector connector;
        try {
            return pool.borrowObject();
        } catch (Exception e) {
            if (e instanceof AccumuloSecurityException) throw (AccumuloSecurityException) e;
            else throw new RuntimeException("Unable to borrow buffer from pool" + e.toString());
        }
    }

    public void closeConnector(Connector connector) {
        try {
            pool.returnObject(connector);
        } catch (Exception e) {
            throw new RuntimeException("Unable to borrow buffer from pool" + e.toString());
        }
    }
}