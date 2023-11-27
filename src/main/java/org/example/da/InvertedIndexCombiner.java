package org.example.da;


import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class InvertedIndexCombiner extends Reducer<Text,Text, Text,Text> {
    private static Text info=new Text();
    @Override
    protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
        int sum=0;
        for(Text value:values){
//            统计单词在单个文档中出现的次数
            sum+=Integer.parseInt(value.toString());
        }
        int splitIndex=key.toString().indexOf(":");
//        将：作为索引标记，为下文分割字符做铺垫
        info.set(key.toString().substring(splitIndex+1)+":"+sum);
//        单词：文件名的格式被划分为    文件名：单词出现次数
        key.set(key.toString().substring(0,splitIndex));
//        将单词单独拿出
        context.write(key,info);
//        写入格式为    单词   文件名：出现的次数
    }

}
