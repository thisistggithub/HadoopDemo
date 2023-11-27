package org.example.da;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class InvertedIndexMapper extends Mapper<LongWritable, Text,Text,Text> {
    private static Text keyInfo=new Text();
    private static final Text valueInfo = new Text("1");
//    定义统计单词数量的代码
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[]fields= StringUtils.split(line, ' ');
//        分隔符号
        FileSplit fileSplit=(FileSplit)context.getInputSplit();
//        定义当前正在操作的文件
        String fileName=fileSplit.getPath().getName();
//        获取当前文件的名称

//        keyInfo作为  单词+文件名
        for(String field:fields){
            keyInfo.set(field+":"+fileName);
            context.write(keyInfo,valueInfo);
//        提交给combine阶段的数据为   单词+文件名  出现次数
        }
    }
}
