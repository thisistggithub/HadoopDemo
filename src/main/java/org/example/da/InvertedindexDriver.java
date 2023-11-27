package org.example.da;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InvertedindexDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf=new Configuration();
        //conf.set("mapreduce.framework.name","yarn");
        Job job=Job.getInstance(conf);
        job.setJarByClass(InvertedindexDriver.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path("D:/daocha/input"));
        FileOutputFormat.setOutputPath(job,new Path("D:/daocha/output"));
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

