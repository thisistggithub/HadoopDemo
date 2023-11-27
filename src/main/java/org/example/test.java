package org.example;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import sun.java2d.loops.FillSpans;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.rmi.server.ExportException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.FileSystem.get;


/**
 * Hello world!
 *
 */
public class test
{
    public static FileSystem fs=null;
    public static void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:9000");
        System.setProperty("HADOOP_USER_NAME","root");
        fs= FileSystem.get(conf);
    }

    public static void testAddFileToHdfs() throws IOException{
        //hdfs dfs -put/-copyFromLocal arc dst;
        Path src= new Path("E:\\HADOOP\\WORD.txt");
        Path dst =new Path("/");
        fs.copyFromLocalFile(src,dst);
    }

    public void testDownloadFileToLocal() throws IllegalArgumentException,IOException{
        fs.copyFromLocalFile(new Path("/WORD.txt"),new Path("E:\\"));
    }

    public void testMkdirAndDeleteAndRename() throws Exception{
        fs.mkdirs(new Path("/a/b/c"));
        fs.mkdirs(new Path("/a2/b2/c2"));
        fs.rename(new Path("/a"),new Path("/a3"));
        fs.delete(new Path("/a2"),true);
    }

    public void testListFiles() throws FileNotFoundException,IllegalArgumentException,IOException{
        RemoteIterator<LocatedFileStatus> listFiles=fs.listFiles(new Path("/car"),true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus=listFiles.next();
            System.out.println("文件名:"+fileStatus.getPath().getName());
            System.out.println("文件的副本数:"+fileStatus.getReplication());
            System.out.println("文件的权限:"+fileStatus.getPermission());
            System.out.println("文件大小:"+fileStatus.getLen()+"字节");
            BlockLocation[] blockLocations=fileStatus.getBlockLocations();
            for (BlockLocation bl:blockLocations){
                String[] hosts=bl.getHosts();
                System.out.println("文件的Block所在虚拟机的主机名:");
                for (String host : hosts){
                    System.out.println(host);
                }
            }
            System.out.println("-----------------------------");
        }
    }

    public static void main( String[] args ) throws IOException {
//        System.out.println( "Hello World!" );
        init();
//      textAddFileToHdfs();
    }
}