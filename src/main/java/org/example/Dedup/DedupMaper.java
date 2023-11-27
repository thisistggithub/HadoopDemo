package org.example.Dedup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DedupMaper extends Mapper <LongWritable,Text,Text,NullWritable>{
    private static Text field = new Text();
    @Override
    protected void map(LongWritable key,Text value,Context context)throws
            IOException,InterruptedException{
        field = value;
        context.write(field, NullWritable.get());
    }
}
