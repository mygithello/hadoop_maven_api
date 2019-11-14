package com.demo.hadoop.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    Text k=new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String s = value.toString();

        //数据清理
        String s1 = ETLUtil.FormatString(s);
        s1 =s1+"ttttttttt";

        //传递数据
        if(s1==null) return;
        k.set(s1);

        context.write(k,NullWritable.get());
    }
}

