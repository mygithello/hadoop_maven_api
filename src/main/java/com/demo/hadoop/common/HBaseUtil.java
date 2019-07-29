package com.demo.hadoop.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @author jyc
 * @title: HBaseUtil
 * @projectName hadoop_maven_api
 * @description:  操作HBase操作
 * 参考连接：https://www.cnblogs.com/zhenjing/p/hbase_example.html
 * @date 2019/7/29  17:23
 */
public class HBaseUtil {

    private static Logger LOG = LoggerFactory.getLogger(HBaseUtil.class);
    private static Configuration conf;
    static {
        conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","master:2181");
        conf.set("master","master:60010");
    }

    /**
     * 创建表  使用createTable函数
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void create(String tableName,String[] columnFamily) throws IOException {
        //创建一个操作表对象
        HBaseAdmin admin =new HBaseAdmin(conf);
        try {
            if (admin.tableExists(tableName)){
                admin.disableTable(tableName);
                admin.deleteTable(tableName);//创建表时小心
            }
            //将表序列化
            HTableDescriptor tableDescriptor=new HTableDescriptor(tableName.valueOf(tableName));
            for (String col : columnFamily) {
                HColumnDescriptor hColumnDescriptor =new HColumnDescriptor(col);
                tableDescriptor.addFamily(hColumnDescriptor);  //列族名
            }
            admin.createTable(tableDescriptor);  //创建表
        }catch (Exception e){
            LOG.error("Create table ["+tableName+"] has error,msg is "+e.getMessage());
        }finally {
            try {
                admin.close();
            }catch (Exception e){
                LOG.error("Close hbase object hass error,msg is "+e.getMessage());
            }
        }
    }


    /**
     * 添加数据  使用put函数
     * @param tableName
     * @param alists
     * @throws IOException
     */
    public static void insert(String tableName, ArrayList<Put> alists) throws IOException {
        //申明连接对象
        HConnection connection = HConnectionManager.createConnection(conf);
        HTableInterface table=connection.getTable(tableName);
        try {
            //判断表是否可用
            if (connection.isTableAvailable(TableName.valueOf(tableName))){
                table.put(alists);
            }else {
                LOG.info("["+tableName+"] table does not exist!");
            }
        }catch (Exception e){
            LOG.info("Add dataset has error,msg is "+e.getMessage());
        }finally {
            try {
                table.close(); //关闭表对象
                connection.close(); //关闭连接对象
            }catch (Exception e){
                LOG.error("Close hbase object has error,msg is "+e.getMessage());
            }
        }
    }

    /**
     * 删除 rowKey下的数据
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void delete(String tableName,String rowKey) throws IOException {
        //申明连接对象
        HConnection connection = HConnectionManager.createConnection(conf);
        HTableInterface table=connection.getTable(tableName);//获取表接口对象
        try {
            if (connection.isTableAvailable(TableName.valueOf(tableName))){
                Delete delete=new Delete(Bytes.toBytes(rowKey));
                table.delete(delete);
            }else {
                LOG.info("["+tableName+"] table does not exist!");
            }
        }catch (Exception e){
            LOG.error("Delete rowKey["+rowKey+"] has error,msg is "+e.getMessage());
        }finally {
            try {
                table.close();
                connection.close();
            }catch (Exception e){
                LOG.error("Close hbase object has error,msg is "+e.getMessage());
            }
        }
    }

    /**
     * 删除表
     * @param tableName
     */
    public static void drop(String tableName) throws IOException {
        //申明连接对象
        HBaseAdmin admin=new HBaseAdmin(conf);
        try {
            //判断表是否存在
            if (admin.tableExists(tableName)){
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }else {
                LOG.info("["+tableName+"] table does not exist!");
            }
        }catch (Exception e){
            LOG.error("Delete table is error,msg is "+e.getMessage());
        }finally {
            try {
                admin.close();
            }catch (Exception e){
                LOG.error("Close hbase object hass error,msg is "+e.getMessage());
            }
        }
    }

    /**
     * 获取数据
     */
    public static void QueryAll(String tableName) {
        HTablePool pool = new HTablePool(conf, 1000);
        HTable table = (HTable) pool.getTable(tableName);
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void QueryByCondition1(String tableName) {

        HTablePool pool = new HTablePool(conf, 1000);
        HTable table = (HTable) pool.getTable(tableName);
        try {
            Get scan = new Get("abcdef".getBytes());// 根据rowkey查询
            Result r = table.get(scan);
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("列：" + new String(keyValue.getFamily())
                        + "====值:" + new String(keyValue.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void QueryByCondition2(String tableName) {
        try {
            HTablePool pool = new HTablePool(conf, 1000);
            HTable table = (HTable) pool.getTable(tableName);
            Filter filter = new SingleColumnValueFilter(Bytes
                    .toBytes("column1"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("aaa")); // 当列column1的值为aaa时进行查询
            Scan s = new Scan();
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void QueryByCondition3(String tableName) {

        try {
            HTablePool pool = new HTablePool(conf, 1000);
            HTable table = (HTable) pool.getTable(tableName);

            List<Filter> filters = new ArrayList<Filter>();

            Filter filter1 = new SingleColumnValueFilter(Bytes
                    .toBytes("column1"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("aaa"));
            filters.add(filter1);

            Filter filter2 = new SingleColumnValueFilter(Bytes
                    .toBytes("column2"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("bbb"));
            filters.add(filter2);

            Filter filter3 = new SingleColumnValueFilter(Bytes
                    .toBytes("column3"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("ccc"));
            filters.add(filter3);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 分页扫描表
     */





    /**
     * ==============================================================================================================
     */

    public static void selectRowKey(String tablename, String rowKey) throws IOException
    {
        HTable table = new HTable(conf, tablename);
        Get g = new Get(rowKey.getBytes());
        Result rs = table.get(g);

        for (KeyValue kv : rs.raw())
        {
            System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
            System.out.println("Column Family: " + new String(kv.getFamily()));
            System.out.println("Column       :" + new String(kv.getQualifier()));
            System.out.println("value        : " + new String(kv.getValue()));
        }
    }

    public static void selectRowKeyFamily(String tablename, String rowKey, String family) throws IOException
    {
        HTable table = new HTable(conf, tablename);
        Get g = new Get(rowKey.getBytes());
        g.addFamily(Bytes.toBytes(family));
        Result rs = table.get(g);
        for (KeyValue kv : rs.raw())
        {
            System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
            System.out.println("Column Family: " + new String(kv.getFamily()));
            System.out.println("Column       :" + new String(kv.getQualifier()));
            System.out.println("value        : " + new String(kv.getValue()));
        }
    }

    public static void selectRowKeyFamilyColumn(String tablename, String rowKey, String family, String column)
            throws IOException
    {
        HTable table = new HTable(conf, tablename);
        Get g = new Get(rowKey.getBytes());
        g.addColumn(family.getBytes(), column.getBytes());

        Result rs = table.get(g);

        for (KeyValue kv : rs.raw())
        {
            System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
            System.out.println("Column Family: " + new String(kv.getFamily()));
            System.out.println("Column       :" + new String(kv.getQualifier()));
            System.out.println("value        : " + new String(kv.getValue()));
        }
    }

    public static void selectFilter(String tablename, List<String> arr) throws IOException
    {
        HTable table = new HTable(conf, tablename);
        Scan scan = new Scan();// 实例化一个遍历器
        FilterList filterList = new FilterList(); // 过滤器List

        for (String v : arr)
        { // 下标0为列簇，1为列名，3为条件
            String[] wheres = v.split(",");

            filterList.addFilter(new SingleColumnValueFilter(// 过滤器
                    wheres[0].getBytes(), wheres[1].getBytes(),

                    CompareFilter.CompareOp.EQUAL,// 各个条件之间是" and "的关系
                    wheres[2].getBytes()));
        }
        scan.setFilter(filterList);
        ResultScanner ResultScannerFilterList = table.getScanner(scan);
        for (Result rs = ResultScannerFilterList.next(); rs != null; rs = ResultScannerFilterList.next())
        {
            for (KeyValue kv : rs.list())
            {
                System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
                System.out.println("Column Family: " + new String(kv.getFamily()));
                System.out.println("Column       :" + new String(kv.getQualifier()));
                System.out.println("value        : " + new String(kv.getValue()));
            }
        }
    }


}
