package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App
{
    public static FileSystem fs=null;//这里的FileSystem要使用org，hadoop的
    public static void init () throws IOException{
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:9000");
        System.setProperty("HADOOP_USER_NAME","root");
        fs=FileSystem.get(conf);
    }
    //上传文件到HDFS
    public static void testAddFileTohdfs() throws IOException{
        //hdfs dsf -put/-copyFromLocal sro dst;
        //指定本地文件系统上传的文件
        //Path src = new Path("D:\\word.txt");
        // src是本地文件路径
        Path src1 = new Path("D:\\word.txt");

        //指定将文件上传到HDFS的目录
        //dst是HDFS指定的目录
        Path dst=new Path("/wordcount/input");//dst 为HDFS的目标路径
        fs.copyFromLocalFile(src1,dst);

        //关闭资源
        // fs.close();
    }

    public static void testDownloadFileToLocal() throws IOException{
        fs.copyToLocalFile(new Path ("/wordcount/input/word.txt"),new Path("D:\\"));

    }

    public static void testMkdirAndDeleteAndRename() throws IOException{//创建删除目录
        fs.mkdirs(new Path("/car"));
//        fs.mkdirs(new Path("/a2/b2/c2"));
//        fs.rename(new Path("/a"),new Path("/a3"));
//        fs.delete(new Path("/a2"),true);
    }

    public static void testListFiles() throws FileNotFoundException,IllegalArgumentException,IOException{
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


    public static void main(String[] args )throws IOException{
        //system.out.println("Hello World!");
        init();
        testAddFileTohdfs();//上传方法
//        testDownloadFileToLocal();//下载方法
//        testMkdirAndDeleteAndRename();
//        testListFiles();

    }
}