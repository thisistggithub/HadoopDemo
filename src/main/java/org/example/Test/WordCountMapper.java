package org.example.Test;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    //每传入一个<k,v>,该方法就被调用一次
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到传入的传入进来的一行内容，把数据类型转化为String
        String line = value.toString();
        //将这一行内容按照分隔符进行一行内容的切割，切割成一个单词数组
        String[] words = line.split(" ");
        //遍历数组，每出现一个单词，就标记一个数字1，<单词，1>
        for(String word:words){
            context.write(new Text(word),new IntWritable(1));
        }//使用mr程序的上下文context，把Map阶段处理的数据发送出去，作为reduce节点的输入数据
    }
}

//在MapReduce的整体流程中，Partitioning、Sorting和Combining这三个阶段的先后顺序如下：
//
//        Partitioning（分区）：
//        在整个Shuffle阶段中，Partitioning是首先发生的阶段。
//        它发生在数据传输到Reducer之前，在数据经过Map阶段输出之后。
//        Partitioning阶段将Map输出的键值对根据键进行分区操作，确定每个键值对要发送到哪个Reducer节点。

//        Sorting（排序）：
//        在Partitioning之后，Sorting是Shuffle阶段的第二个主要阶段。
//        Sorting阶段发生在分区之后，对每个Reducer节点接收到的键值对按键进行排序，以确保相同键的数据被排序在一起。

//        Combining（可选）：
//        Combining是Shuffle阶段的一个可选阶段，它不一定在每个作业中都会发生。
//        如果启用了Combiner，它会在Sorting之后、Reducer节点接收到数据之前，在各个Mapper节点上执行局部聚合操作，
//        对相同键的值进行部分性合并。
//        这个阶段发生在数据发送到Reducer节点之前，用于减少传输到Reducer节点的数据量。
//        所以总体顺序是：Partitioning -> Sorting -> Combining（可选）。这些阶段共同组成了Shuffle阶段，
//        负责将Map阶段的输出重新组织和准备，以供Reducer阶段进行处理。
