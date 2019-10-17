package com.demo.hadoop.common.hbase;
import org.apache.hadoop.hbase.thrift2.generated.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HbaseThriftMain {
    public static void main(String[] args) throws Exception {
        //创建连接
        TTransport transport = new TSocket("10.176.3.72", 9090, 5000);
        TProtocol protocol = new TBinaryProtocol(transport);
        THBaseService.Iface client = new THBaseService.Client(protocol);
        transport.open();

        //指定表名
        ByteBuffer table = ByteBuffer.wrap("stu".getBytes());
        //指定行键
        ByteBuffer row = ByteBuffer.wrap("row1".getBytes());
        //指定列簇
        ByteBuffer family = ByteBuffer.wrap("info".getBytes());
        //指定列名
        ByteBuffer qualifier = ByteBuffer.wrap("name".getBytes());

        //查询
        TGet get = new TGet();
        get.setRow(row);
        TColumn col = new TColumn()
                .setFamily(family) //不指定列簇,即是获取所有列簇
                .setQualifier(qualifier);//不指定列名,即是获取所有列
        get.setColumns(Arrays.asList(col));
        TResult result = client.get(table, get);
        System.out.println(new String(result.getRow()));
        result.getColumnValues().forEach(c -> {
            System.out.println(new String(result.getRow()) + "  " + new String(c.getFamily()) + "_" + new String(c.getQualifier()) + ":" + new String(c.getValue()));
        });

        //批量查询
        TScan scan = new TScan();
        List<TResult> resultList = client.getScannerResults(table, scan, 2);
        resultList.forEach(r -> r.getColumnValues().forEach(c -> {
            System.out.println(new String(r.getRow()) + "  " + new String(c.getFamily()) + "_" + new String(c.getQualifier()) + ":" + new String(c.getValue()));
        }));

        //保存或更新
        TPut put = new TPut();
        put.setRow("row1".getBytes());
        TColumnValue colVal = new TColumnValue();  //定义属性值
        colVal.setFamily(family);
        colVal.setQualifier("name".getBytes());
        colVal.setValue("banana".getBytes());
        put.setColumnValues(Arrays.asList(colVal));
        client.put(table, put);

        //删除
        TDelete delete = new TDelete();
        delete.setRow("row1".getBytes());
//        delete.setColumns(Arrays.asList(new TColumn().setFamily(family).setQualifier("name".getBytes()))); //只删除单个属性
        client.deleteSingle(table, delete);

        transport.close();
    }
}
