package com.rickbranson.mongostress;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.text.DecimalFormat;

import org.apache.commons.cli.*;

import com.mongodb.*;

public class Session
{
    public static final Options availableOptions = new Options();
    public static final String COLLECTION_NAME = "stress";
    private static final String RANDOM_STRING_CHARACTERS = "abcdefghijklmnoprstuvwxyz1234567890";
    private final AtomicInteger executionCounter = new AtomicInteger();
    private final AtomicLong totalRequestMicroseconds = new AtomicLong();

    static
    {
        availableOptions.addOption("h", "help",               false,  "Show this help message and exit");
        availableOptions.addOption("d", "node",               true,   "Host node, default:localhost");
        availableOptions.addOption("p", "port",               true,   "Host port, default:27017");
        availableOptions.addOption("n", "num-keys",           true,   "Number of documents, default:1000000");
        availableOptions.addOption("i", "progress-interval",  true,   "Progress interval, default:10");
        availableOptions.addOption("c", "columns",            true,   "Number of fields per document, default:5");
        availableOptions.addOption("S", "column-size",        true,   "Size of field values in bytes, default:34");
        availableOptions.addOption("t", "threads",            true,   "Number of threads to use, default:50");
        availableOptions.addOption("w", "write-concern",      true,   "Write concern (FSYNC_SAFE, JOURNAL_SAFE, NONE, NORMAL, SAFE), default:NORMAL");
        availableOptions.addOption("Z", "database-name",      true,   "Database name, default:test");
        availableOptions.addOption("Y", "collection-name",    true,   "Collcetion name, default:stress");
    }

    private String optHostname = "";
    private int optCount = 0;
    private int optPort = 0;
    private int optInterval = 0;
    private int optColumns = 0;
    private int optColumnSize = 0;
    private int optThreads = 0;
    private WriteConcern optWriteConcern = null;
    private String optDatabaseName = "";
    private String optCollectionName = "";

    private boolean valid = false;

    public Session(String[] options) throws ParseException 
    {
        try
        {
            CommandLineParser parser  = new PosixParser();
            CommandLine cmd           = parser.parse(availableOptions, options);

            if (cmd.hasOption("h"))
            {
                throw new IllegalArgumentException("help");
            }

            optHostname       = cmd.hasOption("d") ? cmd.getOptionValue("d")                    : "localhost";
            optPort           = cmd.hasOption("p") ? Integer.parseInt(cmd.getOptionValue("p"))  : 27017;
            optCount          = cmd.hasOption("n") ? Integer.parseInt(cmd.getOptionValue("n"))  : 1000000;
            optInterval       = cmd.hasOption("i") ? Integer.parseInt(cmd.getOptionValue("i"))  : 10;
            optColumns        = cmd.hasOption("c") ? Integer.parseInt(cmd.getOptionValue("c"))  : 5;
            optColumnSize     = cmd.hasOption("S") ? Integer.parseInt(cmd.getOptionValue("S"))  : 34;
            optThreads        = cmd.hasOption("t") ? Integer.parseInt(cmd.getOptionValue("t"))  : 50;
            optWriteConcern   = parseWriteConcern(cmd.hasOption("w") ? cmd.getOptionValue("w")  : "NORMAL");
            optDatabaseName   = cmd.hasOption("Z") ? cmd.getOptionValue("Z")                    : "test";
            optCollectionName = cmd.hasOption("Y") ? cmd.getOptionValue("Y")                    : "stress";

            valid = true;
        }
        catch (IllegalArgumentException ex)
        {
            System.out.println("Options:");

            for (Object o : availableOptions.getOptions())
            {
                Option option = (Option) o;
                String upperCaseName = option.getLongOpt().toUpperCase();
                System.out.println(String.format("-%s%s, --%s%s%n\t\t%s%n",
                                    option.getOpt(),
                                    (option.hasArg()) ? (" " + upperCaseName) : "",
                                    option.getLongOpt(),
                                    (option.hasArg()) ? ("=" + upperCaseName) : "",
                                    option.getDescription()));
            }
        }
    }

    public boolean isValid()
    {
        return valid;
    }

    public Mongo createConnection() throws java.net.UnknownHostException
    {
        Mongo m = new Mongo(getHostname(), getPort());
        m.setWriteConcern(getWriteConcern());
        return m;
    }

    public DB getDB(Mongo connection)
    {
        return connection.getDB(getDatabaseName());
    }

    public DBCollection getCollection(Mongo connection)
    {
        return getDB(connection).getCollection(getCollectionName());
    }

    public BasicDBObject nextObject()
    {
        BasicDBObject dbObject = new BasicDBObject();

        for (int i = 0; i < getFieldCount(); i++)
        {
            dbObject.put(Integer.toString(i), randomString(getValueSize()));
        }

        return dbObject; 
    }

    public String getCollectionName()
    {
        return optCollectionName;
    }

    public String getHostname()
    {
        return optHostname; 
    }

    public int getPort()
    {
        return optPort;
    }

    public String getDatabaseName()
    {
        return optDatabaseName;
    }

    public int getRequestCount()
    {
        return optCount; 
    }

    public int getThreadCount()
    {
        return optThreads;
    }

    public int getValueSize()
    {
        return optColumnSize;
    }

    public int getFieldCount()
    {
        return optColumns;
    }

    public WriteConcern getWriteConcern()
    {
        return optWriteConcern;
    }

    public int getStatusInterval()
    {
        return optInterval;
    }

    public boolean incrementExecutionCounter()
    {
        return (executionCounter.incrementAndGet() >= getRequestCount());
    }

    public int getExecutedRequestCount()
    {
        return executionCounter.intValue();
    }

    public void reportRequestLatency(long latencyInNanos)
    {
        totalRequestMicroseconds.addAndGet(latencyInNanos / 1000);
    }

    public long getTotalRequestMicroseconds()
    {
        return totalRequestMicroseconds.get();
    }

    private String randomString(int length)
    {
        Random rng = new Random();
        char[] out = new char[length];

        for (int i = 0; i < length; i++)
        {
            out[i] = RANDOM_STRING_CHARACTERS.charAt(rng.nextInt(RANDOM_STRING_CHARACTERS.length()));
        }

        return new String(out);
    }

    private WriteConcern parseWriteConcern(String input)
    {
        return WriteConcern.valueOf(input);
    }
}


