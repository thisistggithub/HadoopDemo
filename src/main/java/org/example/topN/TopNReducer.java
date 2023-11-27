package org.example.topN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, IntWritable,NullWritable,IntWritable> {
    @Override
    protected void reduce(NullWritable key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException {
        //遍历集合，存入treemap集合,倒叙
        //直接去出前五个
        //将这五个写入上下文
        TreeMap<Integer,String>repToTecordMap=new TreeMap<Integer,String>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2-o1;//返回的是
            }
        });

        for(IntWritable value:values){
            repToTecordMap.put(value.get(),"");
            if(repToTecordMap.size()>5){
                repToTecordMap.remove(repToTecordMap.lastKey());
            }
        }

        for(Integer i:repToTecordMap.keySet()){
            context.write(NullWritable.get(),new IntWritable(i));

        }
    }
}
