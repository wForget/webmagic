package us.codecraft.webmagic.custom.wz.scheduler;

import com.alibaba.fastjson.JSON;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.custom.wz.pipeline.MongoPipeline;
import us.codecraft.webmagic.scheduler.Scheduler;
import us.codecraft.webmagic.utils.HttpConstant;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 2019/2/25.
 */
public class MongoScheduler implements Scheduler {

    private Logger logger = LoggerFactory.getLogger(MongoPipeline.class);

    private MongoClient client = null;
    private String schedulerDb;
    private String schedulerCol;

    public MongoScheduler(String cluster, String db) {
        this(cluster, db, "scheduler");
    }

    public MongoScheduler(String cluster, String db, String col) {
        initMongo(cluster, db, col);
    }


    @Override
    public void push(Request request, Task task) {

        Document doc = toDocument(request);

        Document query = new Document();
        query.put("url", doc.get("url"));
        if (HttpConstant.Method.POST.equals(request.getMethod())) {
            query.put("requestBody.body", doc.get("requestBody.body"));
        }

        Document setDoc = new Document(doc);
        setDoc.put("status", 0);
        Document update = new Document();
        update.put("$set", setDoc);

        getCollection().updateOne(query, update);
    }

    @Override
    public Request poll(Task task) {
        Document query = new Document();
        query.put("status", 0);

        Document setDoc = new Document();
        setDoc.put("status", 1);
        Document update = new Document();
        update.put("$set", setDoc);

        Document doc = getCollection().findOneAndUpdate(query, update);

        return docToRequest(doc);
    }

    private Request docToRequest(Document doc) {
        String json = doc.toJson();
        return JSON.parseObject(json, Request.class);
    }

    private Document toDocument(Object o) {
        String json = JSON.toJSONString(o);
        return Document.parse(json);
    }

    private MongoCollection<Document> getCollection() {
        return client.getDatabase(schedulerDb).getCollection(schedulerCol);
    }
    private void initMongo(String cluster, String db, String col) {
        client = new MongoClient(getServerAddresses(cluster));
        this.schedulerDb = db;
        this.schedulerCol = col;
    }
    private static List<ServerAddress> getServerAddresses(String cluster) {
        List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
        String[] serverAddressStrArr = cluster.split(",");
        for (int i = 0; i < serverAddressStrArr.length; i++) {
            String serverAddressStr = serverAddressStrArr[i];
            String host = serverAddressStr.split(":")[0];
            int port = Integer.valueOf(serverAddressStr.split(":")[1]);
            serverAddresses.add(new ServerAddress(host, port));
        }
        return serverAddresses;
    }
}
