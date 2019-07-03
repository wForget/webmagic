package us.codecraft.webmagic.custom.wz.pipeline;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 2019/2/25.
 *
 * MongoPipeline 向 MongoDB 中写入数据 <br>
 * 在resultItems 中指定 db col，如果没有指定需要在初始化时指定 <br>
 * 在 resultItems 中设置 value 表示保存的数据，类型需要是 WriteModel&lt;Document&gt; 或者 List&lt;WriteModel&lt;Document&gt;&gt; 的实现类型 <br>
 *
 */
public class MongoPipeline implements Pipeline {

    private Logger logger = LoggerFactory.getLogger(MongoPipeline.class);

    private MongoClient client = null;
    private String defaultDb;
    private String defaultCol;

    public MongoPipeline(MongoClient client) {
        this(client, null, null);
    }

    public MongoPipeline(MongoClient client, String db, String col) {
        this.client = client;
        this.defaultDb = db;
        this.defaultCol = col;
    }

    public MongoPipeline(String cluster) {
        this(cluster, null, null);
    }

    public MongoPipeline(String cluster, String db, String col) {
        initMongoClient(cluster);
        this.defaultDb = db;
        this.defaultCol = col;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
        String db = defaultDb;
        String col = defaultCol;
        if (resultItems.get("db") != null) {
            db = resultItems.get("db");
        }
        if (resultItems.get("col") != null) {
            col = resultItems.get("col");
        }

        if (StringUtils.isBlank(db) || StringUtils.isBlank(col)) {
            logger.error("MongoPipeline process error, db or col is blank! db: {}, col: {}", db, col);
            return;
        }

        try {
            Object value = resultItems.get("value");
            List<WriteModel<Document>> writeModelList = new ArrayList<WriteModel<Document>>();
            if (value instanceof List) {
                writeModelList.addAll((List<WriteModel<Document>>) value);
            } else {
                writeModelList.add((WriteModel<Document>) value);
            }

            if (!writeModelList.isEmpty()) {
                MongoCollection<Document> collection = getCollection(db, col);
                collection.bulkWrite(writeModelList, new BulkWriteOptions().ordered(false));
                writeModelList.clear();
            }
        } catch (Exception e) {
            logger.warn("MongoPipeline process error, msg:{}", e);
        }
    }

    private MongoCollection<Document> getCollection(String db, String col) {
        return client.getDatabase(db).getCollection(col);
    }

    private void initMongoClient(String cluster) {
        client = new MongoClient(getServerAddresses(cluster));
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
