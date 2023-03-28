package pers.prover07.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.reduce.InMemoryWriter;

import java.io.IOException;

/**
 * TODO(这里用一句话描述这个类的作用)
 *
 * @author Prover07
 * @date 2023/3/28 17:07
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        // 获取 job
        Job job = Job.getInstance(configuration);

        // 设置 jar 包路径
        job.setJarByClass(WordCountDriver.class);

        // 关联 mapper 和 reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        // 设置 map 输出的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终输出的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(WordCountDriver.class.getClassLoader().getResource("text.txt").getPath()));
        FileOutputFormat.setOutputPath(job, new Path("D:\\StudyCode\\BigData\\Hadoop\\src\\main\\resources\\output"));

        // 提交 job
        boolean wait = job.waitForCompletion(true);

        System.exit(wait ? 0 : 1);
    }

}
