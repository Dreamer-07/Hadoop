package pers.prover07.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * TODO(这里用一句话描述这个类的作用)
 *
 * @author Prover07
 * @date 2023/3/28 16:58
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     *
     * @param key           map 输出的 key
     * @param values        map 相同 key 对应的 values
     * @param context       reduce 上下文环境
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 对 value / key 进行 reduce 操作
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        IntWritable writable = new IntWritable(sum);

        context.write(key, writable);
    }
}
