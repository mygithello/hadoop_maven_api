package com.demo.hadoop.etl;

public class ETLUtil {

    public static String FormatString(String s){

        //切割数据
        String[] split = s.split("\t");

        //过滤脏数据
        if(split.length<9){
            return null;
        }

        //数据替换
        split[3]=split[3].replace(" ","");//将空格去掉

        StringBuilder sb=new StringBuilder();

        //类型拼接
        for(int i=0;i<split.length;i++){
            if(i<9){
                if(i==split.length-1){
                    sb.append(split[i]);
                }else{
                    sb.append(split[i]+"\t");
                }
            }else {
                if(i==split.length-1){
                    sb.append(split[i]);
                }else{
                    sb.append(split[i]+"&");
                }
            }
        }
        return sb.toString();
    }

}
