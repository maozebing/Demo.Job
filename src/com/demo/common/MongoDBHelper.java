package com.demo.common;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * 描述 ：
 *
 * @author : maozebing
 * @version : v1.00
 * @CreationDate : 2017/3/1 11:47
 * @Description :
 * @update : 修改人，修改时间，修改内容
 * @see :[相关类/方法]
 */
public class MongoDBHelper {
    private static MongoDBHelper mongoDB = null;

    /**
     * 实例化
     *
     * @return
     * @throws Exception
     */
    public static MongoDBHelper getInstance() throws Exception {
        if (mongoDB == null) {
            mongoDB = new MongoDBHelper();
        }
        return mongoDB;
    }

    /**
     * 获取数据库连接
     *
     * @param host
     * @param port
     * @return
     * @throws UnknownHostException
     */
    public Mongo getMongo(String host, int port) throws UnknownHostException {
        Mongo mongo = new Mongo(host, port);
        return mongo;
    }

    /**
     * 获取数据库
     *
     * @param mongo
     * @param dbName
     * @return
     */
    public DB getDB(Mongo mongo, String dbName) {
        return mongo.getDB(dbName);
    }

    /**
     * 获取集合
     *
     * @param db
     * @param collection
     * @return
     */
    public DBCollection getCollection(DB db, String collection) {
        return db.getCollection(collection);
    }

    /**
     * 插入数据
     *
     * @throws UnknownHostException
     */
    public void insert(String dataSource, String tableName, List<DBObject> list) throws UnknownHostException {
        Mongo mongo = null;
        try {
            Map<String, String> dataSources = Cache.transTables;
            String host = dataSources.get(dataSource + "_HOST");
            int port = Integer.parseInt(dataSources.get(dataSource + "_PORT"));
            String dbName = dataSources.get(dataSource + "_DATABASE");
            mongo = getMongo(host, port);
            DB db = getDB(mongo, dbName);
            DBCollection dbCollection = getCollection(db, tableName);
            dbCollection.insert(list);
        } finally {
            if (mongo != null) {
                mongo.close();
            }
        }
    }
}
