package hadoop.base.client.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsUtil {
    private String path = "hdfs://hadoop101:9000";
    private String name = "hadoop";

    /**
     * 初始化文件系统
     *
     * @return
     */
    public Configuration initConfig() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", path);
        configuration.set("mapreduce.framework.name", "yarn");
        return configuration;
    }

    /**
     * hdfs操作上传到hdfs 优先级 代码>配置文件>hadoop默认配置文件
     */
    @Test
    public void uploadFileToHdfs() {
        Configuration configuration = initConfig();
        configuration.set("fs.defaultFS", path);
        configuration.set("dfs.replication", "3");
        try {
            FileSystem fs = FileSystem.get(new URI(path), configuration, name);
            fs.copyFromLocalFile(new Path("F://crm2.sql"), new Path("/usr/hadoop/input"));
            fs.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从hdfs中下载文件
     */
    @Test
    public void downloadFileToHdfs() {
        System.setProperty("HADOOP_USER_NAME", "F://work/hadoop/hadoop-2.7.2");
        Configuration configuration = initConfig();
        try {
            FileSystem fs = FileSystem.get(new URI(path), configuration, name);
            // boolean delSrc 指是否将原文件删除
            // Path src 指要下载的文件路径
            // Path dst 指将文件下载到的路径
            // boolean useRawLocalFileSystem 是否开启文件效验
            fs.copyToLocalFile(false, new Path("/usr/hadoop/input/test.txt"), new Path("F://aa/test.txt"), true);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件夹
     */
    @Test
    public void mkdirDicToHdfs() {
        Configuration configuration = initConfig();
        try {
            FileSystem fs = FileSystem.get(new URI(path), configuration, name);
            fs.mkdirs(new Path("/usr/hadoop/output"));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * hdfs删除文件或文件夹
     */
    @Test
    public void deleteToHdfs() {
        Configuration configuration = initConfig();
        try {
            FileSystem fs = FileSystem.get(new URI(path), configuration, name);
            fs.delete(new Path("/usr/hadoop/input/crm2.sql"), true);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 重命名文件夹或文件名
     */
    @Test
    public void renameToHdfs() {
        Configuration configuration = initConfig();
        try {
            FileSystem fs = FileSystem.get(new URI(path), configuration, name);
            fs.rename(new Path("/usr/hadoop/input/test.txt"), new Path("/usr/hadoop/input/test1.txt"));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看文件详情
     */
    @Test
    public void getDetailToHdfs() {
        Configuration configuration = initConfig();
        try {
            FileSystem fs = FileSystem.get(new URI(path), configuration, name);
            //recursive :是否递归
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
            //循环获取文件
            while (files.hasNext()) {
                LocatedFileStatus file = files.next();
                //获取文件
                System.out.println(file.getPath().getName());
                //获取长度
                System.out.println(file.getLen());
                //获取权限
                System.out.println(file.getPermission());
                //获取组
                System.out.println(file.getGroup());
                // 获取存储的块信息
                BlockLocation[] blockLocations = file.getBlockLocations();
                for (BlockLocation blockLocation : blockLocations) {
                    //获取主机信息
                    String[] hosts = blockLocation.getHosts();
                    for (String host : hosts) {
                        System.out.println("主机信息" + host);
                    }
                }
            }
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 用于判断文件或者文件夹
     */
    @Test
    public void booFileOrDicToHdfs() {
        Configuration configuration = initConfig();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(path), configuration, name);
            //recursive :是否递归
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
            if (files.hasNext()){
                if (files.next().isFile()){
                    System.out.println(files.next().getPath().getName()+"是文件");
                }
                if(files.next().isDirectory()){
                    System.out.println(files.next().getPath().getName()+"是文件夹");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
    /**
     * 通过io流进行文件上传
     */
    @Test
    public void uploadIoFileToHdfs(){
        Configuration configuration = initConfig();
        try {
            //获取文件系统
            FileSystem fs = FileSystem.get(new URI(path), configuration, name);
            FileInputStream fileInputStream = new FileInputStream("F://hadoop-2.7.2.zip");
            BufferedInputStream bf = new BufferedInputStream(fileInputStream);
            FSDataOutputStream fsDataOutputStream = fs.create(new Path("/usr/hadoop/hadoop-2.7.2.zip"), true);
            //流对拷
            IOUtils.copyBytes(bf,fsDataOutputStream,configuration);
            // 5 关闭资源
            IOUtils.closeStream(fsDataOutputStream);
            IOUtils.closeStream(fileInputStream);
            IOUtils.closeStream(fs);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
    /**
     * 通过io流进行文件下载
     */
    @Test
    public void downLoadIoFileToHdfs(){
        Configuration configuration = initConfig();
        try {
            FileSystem fs = FileSystem.get(new URI(path), configuration, name);
            FSDataInputStream fsDataInputStream = fs.open(new Path("/usr/hadoop/input/aa.txt"));
            FileOutputStream fileOutputStream =  new FileOutputStream("F://text.txt");
            IOUtils.copyBytes(fsDataInputStream,fileOutputStream,configuration);
            IOUtils.closeStream(fileOutputStream);
            IOUtils.closeStream(fsDataInputStream);
            IOUtils.closeStream(fs);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
    /**
     * 定位文件下载。分块下载
     */
    @Test
    public void locateDownloadToHdfs() throws URISyntaxException, IOException, InterruptedException {
        byte[] b  = new byte[1024*1024];//1M做缓存
        Long blocksize = 1024*1024*128L;
        FileOutputStream out = null;
        Configuration configuration = initConfig();
        FileSystem fs = FileSystem.get(new URI(path), configuration, name);
        FSDataInputStream fsDataInputStream = fs.open(new Path("/uer/hadoop/hadoop-2.7.2.zip"));
        FileStatus fileStatus = fs.getFileStatus(new Path("/uer/hadoop/hadoop-2.7.2.zip"));
        //获取文件块数
        long blockCount = fileStatus.getLen()/blocksize+1;
        //循环下载每一块
        for (int i = 1; i <= blockCount; i++) {
         out = new FileOutputStream("F://test/hadoop-2.7.2.part"+i);
            if(fileStatus.getLen()>=1024*1024*128*i) {
                for (int j = (i-1)*128; j <i*128 ; j++) {
                    fsDataInputStream.read(b);
                    out.write(b);
                }
                fsDataInputStream.seek(1024 * 1024 * 128 * i);
            }else{
                //下载最后一块
                IOUtils.copyBytes(fsDataInputStream,out,configuration);
            }
        }
        IOUtils.closeStream(out);
        IOUtils.closeStream(fsDataInputStream);
        IOUtils.closeStream(fs);
    }
}
