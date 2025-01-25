package fr.ensta.bigdata;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /* START STUDENT CODE */
    static public class Record {
        private LongWritable key;
        private String code;
        private String title;
        private String id;
        private String tag;
        private int total;
        private Map<Integer, Integer> hourly;

        static private boolean objEq(Object str1, Object str2) {
            if (str1 == null) {
                return str2 == null;
            }
            return str1.equals(str2);
        }

        Record(LongWritable key, String code, String title, String id, String tag, int total,
                Map<Integer, Integer> hourly) {
            this.key = key;
            this.code = code;
            this.title = title;
            this.id = id;
            this.tag = tag;
            this.total = total;
            this.hourly = hourly;
        }

        static Map<Integer, Integer> parseHourly(String line) {
            byte[] bytes = line.getBytes();
            Map<Integer, Integer> res = new HashMap<Integer, Integer>();
            int i = 0;

            while (i < bytes.length) {
                int key = bytes[i] - 'A';

                int j = i + 1;
                while (j < bytes.length && '0' <= bytes[j] && bytes[j] <= '9') {
                    j++;
                }

                int val = Integer.parseInt(line.substring(i + 1, j));

                res.put(key, val);
                i = j;
            }

            return Map.copyOf(res);
        }

        static Record parseWithDate(LongWritable lineTag, String line) {
            String[] split = line.split(" ");
            return new Record(
                    lineTag,
                    split[0],
                    split[1],
                    split[2],
                    split[3],
                    Integer.parseInt(split[4]),
                    parseHourly(split[5]));
        }

        @Override
        public boolean equals(Object rec) {
            if (rec == null) {
                return false;
            }

            if (rec.getClass() != this.getClass()) {
                return false;
            }

            final Record other = (Record) rec;
            if (this.code.intern() != other.code.intern()) {
                return false;
            }

            if (!this.code.equals(other.code)) {
                return false;
            }

            return objEq(this.key, other.key) &&
                    objEq(this.code, other.code) &&
                    objEq(this.title, other.title) &&
                    objEq(this.id, other.id) &&
                    objEq(this.tag, other.tag) &&
                    this.total == other.total &&
                    objEq(this.hourly, other.hourly);
        }
    }

    static LocalDate parseInputFileDate(Path path) {
        String name = path.getName();

        Pattern p = Pattern.compile("[0-5]{8}");
        String date = p.matcher(name).results().findFirst().get().group();

        return LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            
        var text = Record.parseWithDate(key, value.toString());

        context.write(new Text(text.code), new IntWritable(text.total));
        return;
    }
    /* END STUDENT CODE */
}
