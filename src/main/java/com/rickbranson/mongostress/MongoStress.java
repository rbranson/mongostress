package com.rickbranson.mongostress;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.text.DecimalFormat;

import org.apache.commons.cli.*;

import com.mongodb.*;

public class MongoStress
{
    public static void main(String[] arguments) throws Exception
    {
        new MongoStress(arguments);
    }

    public MongoStress(String[] arguments) throws Exception
    {
        final Session session = new Session(arguments);

        if (session.isValid())
        {
            final StressRunner runner = new StressRunner(session);
            runner.start();
        }
    }
}
