package com.rickbranson.mongostress;

import com.mongodb.*;

class InsertTask implements StressTask
{
    final BasicDBObject dbObject;

    public InsertTask(BasicDBObject obj)
    {
        dbObject = obj;
    }

    public void perform(StressWorker context)
    {
        dbObject.put("_id", context.getId() + "_" + context.currentRequestId()); 
        context.getCollection().insert(dbObject);
    }
}


