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

public class StressRunner
{
    private final Session session;
    private volatile boolean statusRunning = true;

    public StressRunner(final Session session)
    {
        this.session = session;
        
    }

    public void clearCollection() throws java.net.UnknownHostException
    {
        final Mongo m = session.createConnection();
        final DBCollection coll = session.getCollection(m);
        
        coll.drop();
        m.close();
    }

    public void start() throws Exception
    {
        final ArrayList<StressWorker> workers = new ArrayList<StressWorker>();

        int totalRequests     = session.getRequestCount();
        int totalWorkers      = session.getThreadCount();
        int requestsPerWorker = totalRequests / totalWorkers; 
        int remainderRequests = totalRequests % totalWorkers; 

        clearCollection();

        for (int i = 0; i < totalWorkers; i++)
        {
            StressWorker worker = new StressWorker(session, i, new InsertTask(session.nextObject()));
            workers.add(worker);
        }

        long startTs = System.nanoTime();

        for (StressWorker worker : workers)
        {
            worker.start();
        }

        boolean terminate = false;
        int sleepTime = 25;
        int epoch = 0;
        int interval = session.getStatusInterval();
        int epochIntervals = (interval * 1000) / sleepTime; // problem? 
        long lastReqTotalTime = 0;
        int lastTotal = 0;

        while (!terminate)
        {
            Thread.sleep(sleepTime);

            int alive = 0;

            for (StressWorker worker : workers)
            {
                if (worker.isRunning())
                {
                    alive++;
                }
            }

            if (alive == 0)
            {
                terminate = true;
            }

            epoch++;

            if (terminate || epoch > epochIntervals)
            {
                epoch = 0;

                int total = session.getExecutedRequestCount();
                long reqTotalTime = session.getTotalRequestMicroseconds();

                int reqDelta = total - lastTotal;
                double totalSeconds = (double)(System.nanoTime() - startTs) / (1000 * 1000 * 1000);

                System.out.println(String.format("%d,%.4f,%.1f", total, (double)reqDelta / interval, totalSeconds));

                lastTotal = total;
                lastReqTotalTime = reqTotalTime;
            }
        }

        //double totalSeconds = (double)(System.nanoTime() - startTs) / (1000 * 1000 * 1000);
        //double reqRate = totalRequests / totalSeconds;
        //DecimalFormat df = new DecimalFormat("#.##");

        //System.out.println("Inserted " + totalRequests + " in " + df.format(totalSeconds) + ", " + df.format(reqRate) + "req/s");
    }
}

