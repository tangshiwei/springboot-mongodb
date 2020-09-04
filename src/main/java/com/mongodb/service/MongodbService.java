package com.mongodb.service;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * mongo连接url:
 * <p>
 * mongodb://用户名:密码@localhost/database
 */

@Component
public class MongodbService {

    public MongoClient getMongoClient(String hostname, int port) {
        MongoClient mongoClient = new MongoClient(hostname, port);
        return mongoClient;
    }

    /**
     * 认证获取MongoDB连接
     */
    public MongoClient getMongoClient(String hostname, int port, String username, String password, String databaseName) {
        //ServerAddress()两个参数分别为 服务器地址 和 端口
        ServerAddress serverAddress = new ServerAddress(hostname, port);
        List<ServerAddress> addrs = new ArrayList<>();
        addrs.add(serverAddress);

        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
        MongoCredential credential = MongoCredential.createScramSha1Credential(username, databaseName, password.toCharArray());
        List<MongoCredential> credentials = new ArrayList<>();
        credentials.add(credential);

        //通过连接认证获取MongoDB连接
        MongoClient mongoClient = new MongoClient(addrs, credentials);
        return mongoClient;
    }


    //查询实例
    public void queryDemo(MongoCollection<Document> collection, BasicDBObject query) {
        //时间区间查询 记住如果想根据这种形式进行时间的区间查询 ，存储的时候 记得把字段存成字符串，就按yyyy-MM-dd HH:mm:ss 格式来
        query.put("times", new BasicDBObject("$gte", "2018-06-02 12:20:00").append("$lte", "2018-07-04 10:02:46"));
        //模糊查询
        Pattern pattern = Pattern.compile("^.*王.*$", Pattern.CASE_INSENSITIVE);
        query.put("userName", pattern);
        //精确查询
        query.put("id", "11");
        MongoCursor<Document> cursor = collection.find(query).sort(Sorts.orderBy(Sorts.descending("times"))).skip(0).limit(10).iterator();//
        while (cursor.hasNext()) {
            Document document = cursor.next();
            String s = document.toJson();
        }
    }

    // 获取DB实例
    private MongoDatabase getMongoDatabase(MongoClient mongoClient, String databaseName) {
        if (databaseName == null || "".equals(databaseName.trim())) {
            return null;
        }
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        return database;
    }


    // 获取指定collection表对象
    private MongoCollection<Document> getCollection(MongoClient mongoClient, String dbName, String collName) {
        if (null == collName || "".equals(collName)) {
            return null;
        }
        if (null == dbName || "".equals(dbName)) {
            return null;
        }
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collName);

        return collection;
    }

    // ------------------------------------共用方法---------------------------------------------------

    /**
     * 获取所有数据库名称列表
     */
    public List<String> getDatabaseNames(MongoClient mongoClient) {
        try {
            List<String> list = new ArrayList<>();
            MongoCursor<String> dbsCursor = mongoClient.listDatabaseNames().iterator();
            while (dbsCursor.hasNext()) {
                list.add(dbsCursor.next());
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 查询DB下的所有表名列表
     */
    public List<String> getCollectionNames(MongoClient mongoClient, String dbName) {
        try {
            List<String> list = new ArrayList<>();
            MongoIterable<String> colls = getMongoDatabase(mongoClient, dbName).listCollectionNames();
            MongoCursor<String> iterator = colls.iterator();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
            return list;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除一个数据库
     */
    public void dropDB(MongoClient mongoClient, String dbName) {
        try {
            getMongoDatabase(mongoClient, dbName).drop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据主键_id查找对象
     */
    public Document findById(MongoClient mongoClient, String dbName, String collName, String id) {
        Document myDoc = null;
        try {
            ObjectId objectId = new ObjectId(id);

            MongoCollection<Document> coll = getCollection(mongoClient, dbName, collName);
            myDoc = coll.find(Filters.eq("_id", objectId)).first();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return myDoc;
    }

    /**
     * 统计数
     */
    public Long getCount(MongoClient mongoClient, String dbName, String collName) {
        try {
            MongoCollection<Document> coll = getCollection(mongoClient, dbName, collName);
            Long count = coll.countDocuments();
            return count;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0L;
    }

    /**
     * 条件查询
     */
    public MongoCursor<Document> find(MongoClient mongoClient, String dbName, String collName, Bson filter) {
        try {
            MongoCollection<Document> coll = getCollection(mongoClient, dbName, collName);
            MongoCursor<Document> list = coll.find(filter).iterator();
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 分页查询
     */
    public MongoCursor<Document> findByPage(MongoClient mongoClient, String dbName, String collName, Bson filter, int pageIndex, int pageSize) {
        try {

            Bson orderBy = new BasicDBObject("_id", 1);
            MongoCollection<Document> coll = getCollection(mongoClient, dbName, collName);
            MongoCursor<Document> documentMongoCursor = coll.find(filter).sort(orderBy).skip((pageIndex - 1) * pageSize).limit(pageSize).iterator();

            return documentMongoCursor;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 通过ID删除
     */
    public Long deleteById(MongoClient mongoClient, String dbName, String collName, String id) {
        Long count = null;
        ObjectId objectId = null;
        try {
            objectId = new ObjectId(id);
            Bson filter = Filters.eq("_id", objectId);

            MongoCollection<Document> coll = getCollection(mongoClient, dbName, collName);
            DeleteResult deleteResult = coll.deleteOne(filter);
            count = deleteResult.getDeletedCount();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    /**
     * 更新
     */
    public Document updateById(MongoClient mongoClient, String dbName, String collName, String id, Document newDoc) {
        try {
            ObjectId objectId = new ObjectId(id);

            Bson filter = Filters.eq("_id", objectId);
            MongoCollection<Document> coll = getCollection(mongoClient, dbName, collName);
            // coll.replaceOne(filter, newdoc); // 完全替代
            coll.updateOne(filter, new Document("$set", newDoc));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return newDoc;
    }

    /**
     * 删除collection表
     */
    public void dropCollection(MongoClient mongoClient, String databaseName, String collName) {
        try {
            mongoClient.getDatabase(databaseName).getCollection(collName).drop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭Mongodb
     */
    public void close(MongoClient mongoClient) {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }
    // 数据库创建用户
//    db.createUser(
//    {
//        user: "db",
//                pwd: "jp_123",
//            roles: [ { role: "readWrite", db: "wx_applet" } ]
//    }
//    )

    /**
     * 数据库创建用户
     */
    public void createUser(MongoClient mongoClient, String databaseName, String username, String pwd, String roleName) {
        try {
            MongoDatabase db = mongoClient.getDatabase(databaseName);
            Document doc = new Document();
            doc.append("createUser", username);
            doc.append("pwd", pwd);

            List<Map<String, Object>> mapList = new ArrayList(1);
            Map<String, Object> map = new HashMap<>(2);
            map.put("role", roleName);
            map.put("db", databaseName);
            mapList.add(map);
            doc.append("roles", mapList);

            db.runCommand(doc);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取指定数据库下的所有用户
     */
    public List<Document> getUsers(MongoClient mongoClient, String databaseName) {
        List<Document> list = null;
        try {
            MongoDatabase db = mongoClient.getDatabase(databaseName);
            Document dbStats = new Document("usersInfo", 1);
            Document command = db.runCommand(dbStats);
            list = (List<Document>) command.get("users");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(mongoClient);
        }
        return list;
    }


}
