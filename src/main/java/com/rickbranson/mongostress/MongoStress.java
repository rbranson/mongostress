package com.rickbranson.mongostress;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.mongodb.*;

public class MongoStress
{
    private final Session session;
    private final StressRunner runner;

    public static void main(String[] arguments) throws Exception
    {
        new MongoStress(arguments);
    }

    public MongoStress(String[] arguments) throws Exception
    {
        session = new Session(arguments);
        runner  = new StressRunner(session);

        runner.start();
    }

    class Session
    {
        public static final String COLLECTION_NAME = "stress";
        private static final String RANDOM_STRING_CHARACTERS = "abcdefghijklmnoprstuvwxyz1234567890";

        public Session(String[] options) throws IllegalArgumentException
        {
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

        public String getCollectionName()
        {
            return COLLECTION_NAME;
        }

        public DBCollection getCollection(Mongo connection)
        {
            return getDB(connection).getCollection(getCollectionName());
        }

        public String getHostname()
        {
            return "localhost";
        }

        public int getPort()
        {
            return 27017;
        }

        public String getDatabaseName()
        {
            return "test";
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

        public int getRequestCount()
        {
            return 100000;
        }

        public int getThreadCount()
        {
            return 50;
        }

        public int getValueSize()
        {
            return 64 * 1024;
        }

        public int getFieldCount()
        {
            return 1;
        }

        public WriteConcern getWriteConcern()
        {
            return WriteConcern.SAFE;
        }

        public int getStatusInterval()
        {
            return 10;
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
    }

    class StressWorker
    {
        private final Thread thread;
        private final AtomicInteger counter = new AtomicInteger();

        public StressWorker(final Session session, final int threadId, final int requestCount) throws java.net.UnknownHostException
        {
            final Mongo m           = session.createConnection(); 
            final DBCollection coll = session.getCollection(m); 
            final BasicDBObject obj = session.nextObject();

            thread = new Thread(new Runnable()
            {
                public void run()
                {
                    for (int i = 0; i < requestCount; i++)
                    {
                        obj.put("_id", threadId + "_" + i);
                        coll.insert(obj);
                        counter.getAndIncrement();
                    }

                    m.close();
                }
            });
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

        public int getCounter()
        {
            return counter.intValue();
        }
    } 

    class StressRunner
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

            final Thread statusThread = new Thread(new Runnable()
            {
                public void run() 
                {
                    while (true)
                    {
                        for (int i = 0; i < (session.getStatusInterval() * 1000) / 100; i++)
                        {
                            try
                            {
                                Thread.sleep(100);
                            }
                            catch (InterruptedException ex)
                            {
                                // on purpose yo 
                            }
                            
                            if (!statusRunning)
                            {
                                return;
                            }
                        }

                        int sum = 0;

                        for (StressWorker worker : workers)
                        {
                            sum += worker.getCounter();
                        }

                        System.out.println(sum);
                    }
                }
            });

            clearCollection();

            for (int i = 0; i < totalWorkers; i++)
            {
                StressWorker worker = new StressWorker(session, i, requestsPerWorker + (i == totalWorkers - 1 ? remainderRequests : 0));
                workers.add(worker);
            }

            statusThread.start();

            long startTs = System.nanoTime();

            for (StressWorker worker : workers)
            {
                worker.start();
            }

            for (StressWorker worker : workers)
            {
                worker.awaitTermination();
            }

            statusRunning = false;

            double totalSeconds = (double)(System.nanoTime() - startTs) * 1000 * 1000 * 1000;
            double reqRate = totalRequests / totalSeconds;

            System.out.println("Inserted " + totalRequests + " in " + totalSeconds + ", " + reqRate + "req/s");
        }
    }
}
