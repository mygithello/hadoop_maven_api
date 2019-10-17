package com.demo.hadoop.common.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

public class TestConnectBase {
    public static void main(String[] args) {
        
        //Bucket bucket = cluster2.openBucket("bucket-name");
        //Create your bucket.....
        BucketSettings sampleBucket = new DefaultBucketSettings.Builder()
                .type(BucketType.COUCHBASE)
                .name("bucket-name5")
                .password("123456")
                .quota(200) // megabytes
                .replicas(1)
                .indexReplicas(true)
                .enableFlush(true)
                .build();


        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .connectTimeout(6000)
                .kvTimeout(1000)
                .build();
        Cluster cluster= CouchbaseCluster.create( env , "ip");
       // cluster.authenticate("root","123456");
        /*cluster.clusterManager("couchbase.cluster.username", "couchbase.cluster.password")
                .insertBucket(sampleBucket);*/
        cluster.clusterManager("root","123456").insertBucket(sampleBucket);

        // Just close a single bucket

// Disconnect and close all buckets
        cluster.disconnect();


    /*{
        "email": "anyMail@gmail.com",
            "address": [
        {"city":"ABC"},
        {"city":"DEF"}
  ]
    }*/
        //SELECT * FROM `sampleBucket` WHERE email LIKE "%@gmail.com";




    }


}
