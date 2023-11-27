package org.example.daocha;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import java.io.IOException;

public class daomapper extends Mapper<LongWritable,Text,Text,Text> {
    private static Text keyInfo=new Text();
    private static final Text valueInfo = new Text("1");
//    用来统计单词个数
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到传入的传入进来的一行内容，把数据类型转化为String
        String line = value.toString();
//        定义切片文本
        String[] fields = StringUtils.split(line,' ');


        //将这一行内容按照分隔符进行一行内容的切割，切割成一个单词数组

        FileSplit fileSplit=(FileSplit) context.getInputSplit();
//        定义的是当前正在操作的文件
        String fileName = fileSplit.getPath().getName();
//        获取当前文件的文件名称
        for(String field:fields){
//            定义输出格式为   单词+：+文件名称
            keyInfo.set(field+':'+fileName);
//            将
            context.write(keyInfo,valueInfo);
        }
    }
//    注意，在此之后，会有shuffle阶段，将key值也就是单词相同对应的值，通过逗号连接起来，形成新的键值对
}
