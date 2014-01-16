/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark;

import java.io.Serializable;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import shark.api.Row;
import shark.api.JavaSharkContext;
import shark.api.JavaTableRDD;


// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaAPISuite implements Serializable {

    private static final String WAREHOUSE_PATH = TestUtils$.MODULE$.getWarehousePath();
    private static final String METASTORE_PATH = TestUtils$.MODULE$.getMetastorePath();

    private static transient JavaSharkContext sc;

    @BeforeClass
    public static void oneTimeSetUp() {
        // Intentionally leaving this here since SBT doesn't seem to display junit tests well ...
        System.out.println("running JavaAPISuite ================================================");

        // Check if the SharkEnv's SharkContext has already been initialized. If so, use that to
        // instantiate a JavaSharkContext.
        sc = SharkRunner.initWithJava();

        // test
        sc.sql("drop table if exists test_java");
        sc.sql("CREATE TABLE test_java (key INT, val STRING)");
        sc.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/kv1.txt' INTO TABLE test_java");

        // users
        sc.sql("drop table if exists users_java");
        sc.sql("create table users_java (id int, name string) row format delimited fields terminated by '\t'");
        sc.sql("load data local inpath '${hiveconf:shark.test.data.path}/users.txt' OVERWRITE INTO TABLE users_java");
    }

    @AfterClass
    public static void oneTimeTearDown() {
        sc.stop();
        System.clearProperty("spark.driver.port");
    }

    @Test
    public void selectQuery() {
        List<String> result = sc.sql("select val from test_java");
        Assert.assertEquals(500, result.size());
        Assert.assertTrue(result.contains("val_407"));
    }

    @Test
    public void sql2rdd() {
        JavaTableRDD result = sc.sql2rdd("select val from test_java");
        JavaRDD<String> values = result.map(new Function<Row, String>() {
            @Override
            public String call(Row x) {
                return x.getString(0);
            }
        });
        Assert.assertEquals(500, values.count());
        Assert.assertTrue(values.collect().contains("val_407"));
    }

    @Test
    public void filter() {
        JavaTableRDD result = sc.sql2rdd("select * from users_java");
        JavaTableRDD filtered = result.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return row.getString("name").equals("B");
            }
        }).cache();
        Assert.assertEquals(1, filtered.count());
        Assert.assertEquals(2, filtered.first().getInt("id").intValue());
    }

    @Test
    public void union() {
        JavaTableRDD a = sc.sql2rdd("select * from users_java where name = \"A\"");
        JavaTableRDD b = sc.sql2rdd("select * from users_java where name = \"B\"");
        JavaTableRDD union = a.union(b);
        Assert.assertEquals(3, union.count());
        List<String> uniqueNames = union.map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString("name");
            }
        }).distinct().collect();
        Assert.assertEquals(2, uniqueNames.size());
    }

}
