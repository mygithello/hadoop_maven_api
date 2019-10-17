package com.demo.hadoop.common.hbase;

import org.junit.Test;

import java.io.IOException;

public class TestApiAlone {
    @Test
    public void testCreate() throws IOException {
        String[] strings =new String[1];
        strings[0]="info22";
        for (int i = 0; i < strings.length; i++) {
            String string = strings[i];
            System.out.println(string);
        }
        HBaseUtilAlone.create("test_tmp2",strings);
    }
}
