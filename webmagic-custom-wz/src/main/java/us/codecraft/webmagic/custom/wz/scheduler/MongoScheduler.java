package us.codecraft.webmagic.custom.wz.scheduler;

import com.alibaba.fastjson.JSON;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.scheduler.Scheduler;
import us.codecraft.webmagic.utils.HttpConstant;

import java.util.ArrayList;
import java.util.List;

/**
 * MongoScheduler 使用 Mongo 作为爬虫队列，实现了去重、优先级、状态记录功能 <br>
 */
public class MongoScheduler implements Scheduler {

    private Logger logger = LoggerFactory.getLogger(MongoScheduler.class);

    private MongoClient client = null;
    private String schedulerDb;
    private String schedulerCol;

    public MongoScheduler(MongoClient client, String db) {
        this(client, db, "scheduler");
    }

    public MongoScheduler(MongoClient client, String db, String col) {
        this.client = client;
        this.schedulerDb = db;
        this.schedulerCol = col;
        initCollectionIndex();
    }

    public MongoScheduler(String cluster, String db) {
        this(cluster, db, "scheduler");
    }

    public MongoScheduler(String cluster, String db, String col) {
        initMongo(cluster);
        this.schedulerDb = db;
        this.schedulerCol = col;
        initCollectionIndex();
    }


    @Override
    public void push(Request request, Task task) {

        Document doc = toDocument(request);

        Document query = new Document();
        query.put("url", doc.get("url"));
        if (HttpConstant.Method.POST.equals(request.getMethod())) {
            Object body = doc.get("requestBody.body");
            if (body != null) {
                query.put("requestBody.body", body);
            }
        }

        Document setDoc = new Document(doc);
        setDoc.put("status", 0);
        Document update = new Document();
        update.put("$setOnInsert", setDoc);

        getCollection().updateOne(query, update, new UpdateOptions().upsert(true));
    }

    @Override
    public Request poll(Task task) {
        Document query = new Document();
        query.put("status", 0);

        Document setDoc = new Document();
        setDoc.put("status", 1);
        Document update = new Document();
        update.put("$set", setDoc);

        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().sort(new Document("priority", -1));

        Document doc = getCollection().findOneAndUpdate(query, update, options);
        if (doc == null || doc.isEmpty()) return null;

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

    public void changeRequestStatus(Request request, Integer status) {
        Document query = new Document();
        query.put("url", request.getUrl());
        if (HttpConstant.Method.POST.equals(request.getMethod())) {
            byte[] body = request.getRequestBody().getBody();
            if (body != null) {
                query.put("requestBody.body", body);
            }
        }

        Document setDoc = new Document();
        setDoc.put("status", status);
        Document update = new Document();
        update.put("$set", setDoc);

        getCollection().updateOne(query, update);
    }

    private MongoCollection<Document> getCollection() {
        return client.getDatabase(this.schedulerDb).getCollection(this.schedulerCol);
    }
    private void initMongo(String cluster) {
        client = new MongoClient(getServerAddresses(cluster));

    }
    private void initCollectionIndex() {
        Document indexDoc = new Document();
        List<IndexModel> indexModels = new ArrayList<IndexModel>();
        indexModels.add(new IndexModel(new Document("url", 1)));
        indexModels.add(new IndexModel(new Document("status", 1)));
        client.getDatabase(this.schedulerDb).getCollection(this.schedulerCol).createIndex(indexDoc);
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
