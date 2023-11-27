package org.example.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    //把job提交给集群去运行
    public static void main(String[] args) throws Exception{
//        System.setProperty("HADOOP_USER_NAME", "root");
        Job job = Job.getInstance(new Configuration());
        //指定我这个job所在的jar包
        job.setJarByClass(WordCountDriver.class);
        //指定本次mr 所用的mapper reduce类分别是什么
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //指定本次mr mapper阶段的输出 k v类型
        job.setMapOutputKeyClass(Text.class);//Text用于表示文本字符串的数据类型，类似于Java中的String类型。
        job.setMapOutputValueClass(IntWritable.class);//IntWritable用于表示整数值的数据类型，类似于Java中的Integer类型。
        //指定本次mr 最终输出的 k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定本次mr 输入的数据路径，和最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交程序，并且监控打印程序执行的结果
//        boolean b = job.waitForCompletion(true);
//        System.exit(b?0:1);
        boolean b = job.waitForCompletion(true);
        if (b) {
            System.out.println();
            System.out.println("田总提醒您：jar包运行完成，没有任何问题");
            System.out.println("田总提醒您：你可以继续运行你的代码啦！！！");
        } else {
            System.out.println("田总提醒您：jar包运行失败，请查找问题");
            System.out.println("田总提醒您：jar包运行失败，应该是你虚拟机的问题");
            System.out.println("田总提醒您：因为MapRduce代码是我自己写的，不会有任何问题");
        }
    }
}