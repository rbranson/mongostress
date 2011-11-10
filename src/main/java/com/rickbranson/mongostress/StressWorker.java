package com.rickbranson.mongostress;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.text.DecimalFormat;

import org.apache.commons.cli.*;

import com.mongodb.*;

public class StressWorker
{
    private final Thread thread;
    private final Mongo connection;
    private final DBCollection coll;
    private final int id;
    private int counter = 0;

    public StressWorker(final Session session, final int threadId, final StressTask task) throws java.net.UnknownHostException
    {
        final StressWorker worker = this;

        connection  = session.createConnection();
        coll        = session.getCollection(connection); 
        id          = threadId;

        thread = new Thread(new Runnable()
        {
            public void run()
            {
                boolean run = true;
                int i       = 0;
                long beforeTime;
                long latency;

                while (run)
                {
                    run         = !session.incrementExecutionCounter();
                    beforeTime  = System.nanoTime();

                    counter++;

                    task.perform(worker);

                    latency = (System.nanoTime() - beforeTime) / (1000 * 1000 * 1000);
                    session.reportRequestLatency(latency);
                }

                connection.close();
            }
        });
    }

    public int getId()
    {
        return id;
    }

    public int currentRequestId()
    {
        return counter;
    }

    public Mongo getConnection()
    {
        return connection;
    }

    public DBCollection getCollection()
    {
        return coll;
    }

    public boolean isRunning()
    {
        return thread.isAlive();
    }

    public void start()
    {
        thread.start();
    }

    public void awaitTermination()
    {
        try
        {
            thread.join();
        }
        catch (InterruptedException ex)
        {
        }
    }
} 


