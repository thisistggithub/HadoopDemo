package org.example.Test;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text,IntWritable> {
    @Override
    //reduce接收所有来自map阶段处理的数据之后，按照key的字典序进行排序
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //定义一个计数器
        int count = 0;
        //遍历一组迭代器，把每一个数量1累加起来就构成了单词的总次数
        //Shuffle过程中，相同键的值会被收集在一起,形成新的集合
        for(IntWritable value:values){
            count += value.get();
        }
        //把最终结果输出
        context.write(key,new IntWritable(count));
    }
}