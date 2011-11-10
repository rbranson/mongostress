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
    public static final int SLEEP_TICK_TIME = 25;
    private final Session session;
    private volatile boolean statusRunning = true;

    public StressRunner(final Session session)
    {
        this.session = session;
    }

    private void prepare()
    {
        String op = session.getOperation();

        if (op.equalsIgnoreCase("INSERT"))
        {
            InsertTask.prepare(session);
        }
        else if (op.equalsIgnoreCase("READ"))
        {
            GetTask.prepare(session);
        }
    }

    private StressTask nextTask() throws Exception
    {
        String op = session.getOperation();

        if (op.equalsIgnoreCase("INSERT"))
        {
            return new InsertTask(session.nextObject());
        }
        else if (op.equalsIgnoreCase("READ"))
        {
            return new GetTask();
        }
        else
        {
            throw new Exception("Unknown operation provided: " + op);
        }
    }

    private boolean areWorkersDead(Collection<StressWorker> workers)
    {
        for (StressWorker worker : workers)
        {
            if (worker.isRunning())
            {
                return false; 
            }
        }
        
        return true;
    }

    public void start() throws Exception
    {
        final ArrayList<StressWorker> workers = new ArrayList<StressWorker>();

        prepare();

        for (int i = 0; i < session.getThreadCount(); i++)
        {
            workers.add(new StressWorker(session, i, nextTask()));
        }

        final long startTs        = System.nanoTime();
        final int interval        = session.getStatusInterval();
        final int epochIntervals  = (interval * 1000) / SLEEP_TICK_TIME; // problem? 

        boolean terminate         = false;
        int epoch                 = 0;
        long lastReqTotalTime     = 0;
        int lastTotal             = 0;

        for (StressWorker worker : workers)
        {
            worker.start();
        }

        System.out.println("total,interval_op_rate,avg_latency,elapsed_time");

        while (!terminate)
        {
            Thread.sleep(SLEEP_TICK_TIME);

            if (areWorkersDead(workers))
            {
                terminate = true;
            }

            epoch++;

            if (terminate || epoch > epochIntervals)
            {
                epoch = 0;

                int total         = session.getExecutedRequestCount();
                long reqTotalTime = session.getTotalRequestMicroseconds();

                int reqDelta                = total - lastTotal;
                long reqTimeDelta           = reqTotalTime - lastReqTotalTime;
                double reqTimeDeltaSeconds  = (double)reqTimeDelta / (1000 * 1000);
                double totalSeconds         = (double)(System.nanoTime() - startTs) / (1000 * 1000 * 1000);
                double latencyDelta         = reqDelta == 0 ? reqTimeDeltaSeconds : reqTimeDeltaSeconds / reqDelta;

                System.out.println(String.format("%d,%d,%.6f,%.1f", total, reqDelta / interval, latencyDelta, totalSeconds));

                lastTotal = total;
                lastReqTotalTime = reqTotalTime;
            }
        }
    }
}

