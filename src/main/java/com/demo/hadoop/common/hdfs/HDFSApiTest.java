package com.demo.hadoop.common.hdfs;

import org.junit.Test;

import java.io.IOException;

public class HDFSApiTest {

    @Test
    public void testLs() throws IOException {
        System.setProperty("hadoop.home.dir","D:\\bigdata\\hadoop\\hadoop-2.7.4" );
        HDFSUtil.ls("/");
        System.out.println("000000");
    }

    @Test
    public void testMkdir() throws IOException {
        //System.setProperty("hadoop.home.dir","D:\\bigdata\\hadoop\\hadoop-2.7.4" );
        System.setProperty("HADOOP_USER_NAME","hadoop");
        //export HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib/native"
        System.setProperty("HADOOP_OPTS","-Djava.library.path=D:\\bigdata\\hadoop\\hadoop-2.7.4\\lib\\native");
        /**
         * 1.修改当前操作系统的用户名为linux文件系统中的用户名(此种方法最low)
         * 2. 通过 haddop fs -chmod 777 ... 修改文件的权限为任何人可读可写(不安全很low)
         * 3.显示设定环境变量： 比较靠谱的方式：System.setProperty("HADOOP_USER_NAME", "root"); 当然还可以通过run configuration 的方式进行指定jvm环境功能变量‘-DHADOOP_USER_NAME=hadoop’
         */
        //tmp/test2
        //HDFSUtil.mkdir("/user/hadoop/test22/");
        HDFSUtil.mkdir("/user/sqoop/datatest");
        System.out.println("000000");
    }

    @Test
    public void testPut() throws IOException {
        System.setProperty("hadoop.home.dir","D:\\bigdata\\hadoop\\hadoop-2.7.4" );
        System.setProperty("HADOOP_USER_NAME","hadoop");
        /**
         * 1.修改当前操作系统的用户名为linux文件系统中的用户名(此种方法最low)
         * 2. 通过 haddop fs -chmod 777 ... 修改文件的权限为任何人可读可写(不安全很low)
         * 3.显示设定环境变量： 比较靠谱的方式：System.setProperty("HADOOP_USER_NAME", "root"); 当然还可以通过run configuration 的方式进行指定jvm环境功能变量‘-DHADOOP_USER_NAME=hadoop’
         */
        HDFSUtil.put("/user/sqoop","D:\\ddd.txt");
    }


    @Test
    public void testGet() throws IOException {
        //System.setProperty("hadoop.home.dir","D:\\bigdata\\hadoop\\hadoop-2.7.4" );
        System.setProperty("HADOOP_USER_NAME","hadoop");
        //export HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib/native"
        System.setProperty("HADOOP_OPTS","-Djava.library.path=D:\\bigdata\\hadoop\\hadoop-2.7.4\\lib\\native");
        /**
         * 1.修改当前操作系统的用户名为linux文件系统中的用户名(此种方法最low)
         * 2. 通过 haddop fs -chmod 777 ... 修改文件的权限为任何人可读可写(不安全很low)
         * 3.显示设定环境变量： 比较靠谱的方式：System.setProperty("HADOOP_USER_NAME", "root"); 当然还可以通过run configuration 的方式进行指定jvm环境功能变量‘-DHADOOP_USER_NAME=hadoop’
         */
        //对应下载 hdfs://cluster1/user/hadoop/test22====>/user/hadoop/test22
        HDFSUtil.get("test22","D:\\testlocal");
        System.out.println("000000");
    }

    @Test
    public void testCat() throws IOException {
        //System.setProperty("hadoop.home.dir","D:\\bigdata\\hadoop\\hadoop-2.7.4" );
        System.setProperty("HADOOP_USER_NAME","hadoop");
        //export HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib/native"
        System.setProperty("HADOOP_OPTS","-Djava.library.path=D:\\bigdata\\hadoop\\hadoop-2.7.4\\lib\\native");
        /**
         * 1.修改当前操作系统的用户名为linux文件系统中的用户名(此种方法最low)
         * 2. 通过 haddop fs -chmod 777 ... 修改文件的权限为任何人可读可写(不安全很low)
         * 3.显示设定环境变量： 比较靠谱的方式：System.setProperty("HADOOP_USER_NAME", "root"); 当然还可以通过run configuration 的方式进行指定jvm环境功能变量‘-DHADOOP_USER_NAME=hadoop’
         */
        //对应下载 hdfs://cluster1/user/hadoop/test22====>/user/hadoop/test22
        HDFSUtil.cat("test22");
        System.out.println("000000");
    }

    @Test
    public void testRmr() throws IOException {
        //System.setProperty("hadoop.home.dir","D:\\bigdata\\hadoop\\hadoop-2.7.4" );
        System.setProperty("HADOOP_USER_NAME","hadoop");
        //export HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib/native"
        System.setProperty("HADOOP_OPTS","-Djava.library.path=D:\\bigdata\\hadoop\\hadoop-2.7.4\\lib\\native");
        /**
         * 1.修改当前操作系统的用户名为linux文件系统中的用户名(此种方法最low)
         * 2. 通过 haddop fs -chmod 777 ... 修改文件的权限为任何人可读可写(不安全很low)
         * 3.显示设定环境变量： 比较靠谱的方式：System.setProperty("HADOOP_USER_NAME", "root"); 当然还可以通过run configuration 的方式进行指定jvm环境功能变量‘-DHADOOP_USER_NAME=hadoop’
         */
        //对应下载 hdfs://cluster1/user/hadoop/test22====>/user/hadoop/test22
        HDFSUtil.rmr("test22");
        System.out.println("000000");
    }


}
