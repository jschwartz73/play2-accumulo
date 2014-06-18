package com.schwartech.pool;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.commons.pool2.ObjectPool;

/**
 * Created by jeff on 6/17/14.
 */
public class AccumuloConnectorUtil {
    private ObjectPool<Instance> pool;

    public AccumuloConnectorUtil(ObjectPool<Instance> pool) {
        this.pool = pool;
    }

    public Connector getConnector(String username, AuthenticationToken token) throws AccumuloSecurityException, AccumuloException {
        Instance instance;
        try {
            instance = pool.borrowObject();
            return instance.getConnector(username, token);
        } catch (Exception e) {
            if (e instanceof AccumuloSecurityException) throw (AccumuloSecurityException)e;
            else throw new RuntimeException("Unable to borrow buffer from pool" + e.toString());
        } finally {
            try {
                if (null != instance) {
                    pool.returnObject(instance);
                }
            } catch (Exception e) {
                // ignored
            }
    }
}
