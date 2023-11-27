package org.example.daocha;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class daocom extends Reducer<Text,Text,Text,Text> {
//    辞此操作在shuffle后
    private static Text info = new Text();
    protected void  reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
        int sum=0;
        for (Text value :values){
            sum+=Integer.parseInt(value.toString());
        }
        int splitIndex = key.toString().indexOf(":");
//        获取”：“所在位置
        info.set(key.toString().substring(splitIndex+1)+":"+sum);
//        substring方法时选取区间左闭右开字符串
        key.set(key.toString().substring(0,splitIndex));
        context.write(key,info);

    }

}
