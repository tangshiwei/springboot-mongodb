package com.mongodb;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.service.MongodbService;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Set;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestMongodb {
    @Autowired
    private MongodbService mongodbService;


    @Test
    public void test() throws Exception {
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoIterable<String> iterable = mongoClient.listDatabaseNames();
        MongoCursor<String> dbsCursor = mongoClient.listDatabaseNames().iterator();
        while (dbsCursor.hasNext()) {
            System.out.println(dbsCursor.next());
        }

    }

    @Test
    public void testObj() throws Exception {

        MongoClient mongoClient = mongodbService.getMongoClient("localhost", 27017);
        // mongodbService.createUser(mongoClient,"admin","admin","123456","dbAdmin");
        List<String> dbList = mongodbService.getDatabaseNames(mongoClient);


        dbList.forEach(db -> {
            System.out.println("databaseName:" + db);

            List<String> tableList = mongodbService.getCollectionNames(mongoClient, db);
            if (tableList != null)
                tableList.forEach(t -> System.out.println("\t tableName:" + t));
        });


    }

}
