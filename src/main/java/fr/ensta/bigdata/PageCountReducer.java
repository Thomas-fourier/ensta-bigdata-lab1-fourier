package fr.ensta.bigdata;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /* START STUDENT CODE */
    void reduce(Text key, List<IntWritable> values, Context context) throws IOException, InterruptedException {
        int res = 0;
        for (IntWritable i : values) {
            res = res + i.get();   
        }
        context.write(key, new IntWritable(res));
    }
    /* END STUDENT CODE */
}
