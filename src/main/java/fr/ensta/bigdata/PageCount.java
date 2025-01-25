package fr.ensta.bigdata;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageCount {
    /* START STUDENT CODE */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        

      /*
       * To make our program more flexible, we'll allow the input
       * and output directory paths to be specified on the command
       * line instead of hardcoding them. The first thing our driver
       * will do is verify that we were passed these arguments (and
       * ONLY these arguments).
       */
       if (args.length != 2) {
          System.out.printf("Usage: Driver <input dir> <output dir>\n");
          System.exit(-1);
       }

      /*
       * Instantiate a Job object for our job's configuration.  
       */
      Job job = Job.getInstance();

      /*
       * Specify the paths to the input and output data based on the
       * command-line arguments.
       */
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      /*
       * Specify the JAR (Java archive) file containing your driver, mapper, 
       * and reducer.  Hadoop will transfer this JAR file to nodes in your 
       * cluster that run the map and reduce tasks. This method instructs
       * Hadoop to find the JAR file based on a specific class it contains.
       */
      // job.setJarByClass(Driver.class);
    
      /*
       * Explicitly setting a descripive name for the job will help us to
       * more easily identify our job in reports and log files, especially 
       * on a busy cluster that runs lots of jobs from many users.
       */
      job.setJobName("Employee Salary Analysis Driver");

      /*
       * Tell Hadoop which mapper and reducer classes we'll use for 
       * this job.
       */
      job.setMapperClass(PageCountMapper.class);
      job.setReducerClass(PageCountReducer.class);

      /*
       * Specify the job's output key and value classes.
       */
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

      /*
       * Start the MapReduce job and wait for it to finish.
       * If it finishes successfully, return 0. If not, return 1.
       */
      boolean success = job.waitForCompletion(true);
      System.exit(success ? 0 : 1);
    }
    /* END STUDENT CODE */
}
