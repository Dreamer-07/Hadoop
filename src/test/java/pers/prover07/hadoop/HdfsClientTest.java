package pers.prover07.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * TODO(这里用一句话描述这个类的作用)
 *
 * @author Prover07
 * @date 2023/3/28 14:16
 */
public class HdfsClientTest {

    private FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        // 返回主机名而不是ip地址
        configuration.set("dfs.client.use.datanode.hostname", "true");

        // 链接
        fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "root");
    }

    // 关闭链接
    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    @Test
    public void testMkdir() throws IOException {
        fileSystem.mkdirs(new Path("/java"));
    }

    /**
     * 上传
     *
     * @throws IOException
     */
    @Test
    public void testCopyFromLocalFile() throws IOException {
        // 是否删除源数据，是否覆盖，本地文件地址，hdfs文件地址
        fileSystem.copyFromLocalFile(false, true, new Path("D:/StudyCode/BigData/Hadoop/README.md"), new Path("/java"));
    }

    /**
     * 下载
     *
     * @throws IOException
     */
    @Test
    public void testCopyToLocalFile() throws IOException {
        // 是否删除源数据，hdfs文件地址，本地文件地址，是否开启文件校验
        fileSystem.copyToLocalFile(false, new Path("/java/README.md"), new Path("D:\\"), true);
    }

    /**
     * 文件更名和移动
     *
     * @throws IOException
     */
    @Test
    public void testMv() throws IOException {
        fileSystem.rename(new Path("/java/README.md"), new Path("/java/test.md"));
        // 也可以对目录进行更名
    }

    /**
     * 查看文件详情
     */
    @Test
    public void testDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

        // 遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }

    }


    /**
     * 文件和文件夹判断
     * @throws IOException
     */
    @Test
    public void testFileType() throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
    }

}
