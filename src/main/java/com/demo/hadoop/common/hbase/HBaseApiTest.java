package com.demo.hadoop.common.hbase;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class HBaseApiTest {

    /**
     * 测试创建表
     */
    @Test
    public void testCreate() throws IOException {
        String[] strings =new String[1];
        strings[0]="info22";
        for (int i = 0; i < strings.length; i++) {
            String string = strings[i];
            System.out.println(string);
        }
        HBaseUtil.create("test_tmp",strings);
    }

    /**
     * 测试添加数据
     */
    @Test
    public void testPut() throws IOException {
        ArrayList<Put> alists =new ArrayList<>();

        Put put = new Put(Bytes.toBytes("100001"));
        put.add(Bytes.toBytes("info22"), Bytes.toBytes("name"), Bytes.toBytes("lion"));
        put.add(Bytes.toBytes("info22"), Bytes.toBytes("address"), Bytes.toBytes("shangdi"));
        put.add(Bytes.toBytes("info22"), Bytes.toBytes("age"), Bytes.toBytes("30"));
        put.setDurability(Durability.SYNC_WAL);

        alists.add(put);
        HBaseUtil.put("test_tmp",alists);
    }


    /**
     * 测试查询
     */
    @Test
    public void testQuaryAll() throws IOException {
        HBaseUtil.queryAll("test_tmp");
    }

    /**
     * 查询单行记录  by rowkey
     */
    @Test
    public void testSelectRowKey() throws IOException {
        //测试过程中发现，rowkey写错，找不到数据，不会报错
        HBaseUtil.selectRowKey("test_tmp","100001");
    }


    /**
     * 查询单行记录  by rowkey +family
     */
    @Test
    public void testSelectRowKeyFamily() throws IOException {
        //测试过程中发现，rowkey写错，找不到数据，不会报错//family参数写错，会报错，该列簇不存在
        HBaseUtil.selectRowKeyFamily("test_tmp","100001","info22");
    }

    /**
     * 查询单行记录  by rowkey +family+column
     */
    @Test
    public void testSelectRowKeyFamilyColumn() throws IOException {
        //测试过程中发现，rowkey写错，找不到数据，不会报错//family参数写错，会报错，该列簇不存在
        HBaseUtil.selectRowKeyFamilyColumn("test_tmp","100001","info22","name");
    }


    /**
     * 删除rowkey下的数据
     */
    @Test
    public void testDelete() throws IOException {
        HBaseUtil.delete("test_tmp","100001");
    }



    /**
     * 删除表
     */
    @Test
    public void testDrop() throws IOException {
        HBaseUtil.drop("test_tmp");
    }



}
