package com.demo.hadoop.etl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ETLDriver implements Tool {

    private Configuration configuration;

    @Override
    public int run(String[] strings) throws Exception {
        //创建Job
        Job job = Job.getInstance(configuration);

        //设置运行环境
        job.setJarByClass(ETLDriver.class);

        //设置对应的MapperReduce类
        job.setMapperClass(ETLMapper.class);

        //设置Mapper输出的
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置全局的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输出输入路径
        //FileInputFormat.setInputPaths(job,new Path(strings[0]));
        FileInputFormat.setInputPaths(job,new Path("D:\\idea_room\\hadoop_maven_api\\src\\main\\java\\com\\demo\\hadoop\\etl\\test_input.txt"));
        //FileOutputFormat.setOutputPath(job,new Path(strings[1]));
        FileOutputFormat.setOutputPath(job,new Path("D:\\idea_room\\hadoop_maven_api\\src\\main\\java\\com\\demo\\hadoop\\etl\\out"));

        //不需要reduce
        job.setNumReduceTasks(0);

        //提交
        job.submit();
        return 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration=configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    //主函数
    public static void main(String[] args) throws Exception{
        ToolRunner.run(new ETLDriver(),args);
    }
}