package org.example.da;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {
    private static Text result=new Text();
    @Override
    protected void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException {
        String fileList=new String();
        for(Text value:values){
            fileList+=value.toString()+";";
//            将提交后的  单词  文件名:出现次数   以;做结尾
//            并且不断使用字符串累加原则不断迭代
        }

//        最后打印出来
        result.set(fileList);
        context.write(key,result);
    }
}
