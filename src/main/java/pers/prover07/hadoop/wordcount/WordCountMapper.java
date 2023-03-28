package pers.prover07.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TODO(这里用一句话描述这个类的作用)
 *
 * @author Prover07
 * @date 2023/3/28 16:51
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text reduceKey = new Text();

    private IntWritable reduceValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 获取一行数据
        String str = value.toString();
        // 切割数据
        String[] strArr = str.split(" ");

        for (String s : strArr) {
            // 设置 key 值
            reduceKey.set(s);
            // 写出
            context.write(reduceKey, reduceValue);
        }
    }
}
