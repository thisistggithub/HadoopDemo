package org.example.daocha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.Test.WordCountDriver;
import org.example.Test.WordCountMapper;
import org.example.Test.WordCountReducer;

public class daodriver {
    public static void main(String[] args) throws Exception {
//        System.setProperty("HADOOP_USER_NAME", "root");
        Job job = Job.getInstance(new Configuration());
        //指定我这个job所在的jar包
        job.setJarByClass(daodriver.class);
        //指定本次mr 所用的mapper reduce类分别是什么
        job.setMapperClass(daomapper.class);
        job.setReducerClass(daoreduce.class);
        //指定本次mr mapper阶段的输出 k v类型
        job.setMapOutputKeyClass(Text.class);//Text用于表示文本字符串的数据类型，类似于Java中的String类型。
        job.setMapOutputValueClass(IntWritable.class);//IntWritable用于表示整数值的数据类型，类似于Java中的Integer类型。
        //指定本次mr 最终输出的 k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定本次mr 输入的数据路径，和最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交程序，并且监控打印程序执行的结果
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
