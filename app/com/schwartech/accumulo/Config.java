package com.schwartech.accumulo;

import org.apache.accumulo.core.client.BatchWriterConfig;

import java.util.concurrent.TimeUnit;

/**
 * Created by jeff on 10/26/15.
 */
public class Config {
    public String username;
    public String password;
    public long memBuf;
    public long timeout;
    public int numThreads;
    public String instanceName;
    public String zooServers;

    private BatchWriterConfig batchWriterConfig = null;

    public BatchWriterConfig getBatchWriterConfig() {
        if (batchWriterConfig== null) {
            batchWriterConfig = new BatchWriterConfig()
                    .setMaxMemory(memBuf)
                    .setMaxWriteThreads(numThreads)
                    .setTimeout(timeout, TimeUnit.MILLISECONDS);

        }
        return batchWriterConfig;
    }

}
