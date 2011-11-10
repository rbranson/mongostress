/*
 * Copyright (c) 2011, Richard W. Branson 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

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
    private final Session session;
    private final int id;
    private int counter = 0;
    private static final Random rng = new Random();

    public StressWorker(final Session session, final int threadId, final StressTask task) throws java.net.UnknownHostException
    {
        final StressWorker worker = this;

        this.session  = session;
        connection    = session.createConnection();
        coll          = session.getCollection(connection); 
        id            = threadId;

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

                    session.reportRequestLatency(System.nanoTime() - beforeTime);
                }

                connection.close();
            }
        });
    }

    public String currentRequestKey()
    {
        return getId() + "_" + currentRequestId();
    }

    public String randomRequestKey()
    {
        return getId() + "_" + rng.nextInt(session.getRequestCount() / session.getThreadCount());
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


