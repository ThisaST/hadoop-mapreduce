package com.ktsession.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 *
 */
public class WordCount
{
    public static void main( String[] args ) {

        if (args.length < 2) {

            System.err.println("Please provide the dataset input directory and the output directory");
            System.exit(0);
        }

        Configuration configuration = new Configuration();

        try {

            //Define mapreduce job
            Job mapReduceWordcountJob = Job.getInstance(configuration, "word count");
            mapReduceWordcountJob.setJarByClass(WordCount.class);
            mapReduceWordcountJob.setJobName("Word count");

            //Set input location where the data set is located
            FileInputFormat.addInputPath(mapReduceWordcountJob, new Path(args[0]));


            //set output location or result of the mapreduce job
            FileOutputFormat.setOutputPath(mapReduceWordcountJob, new Path(args[1]));


            mapReduceWordcountJob.setOutputKeyClass(Text.class);

            //set output format

            //set mapper class
            mapReduceWordcountJob.setMapperClass(WordCountMapper.class);

            //set the reducer class
            mapReduceWordcountJob.setReducerClass(WordCountReducer.class);


            mapReduceWordcountJob.setCombinerClass(WordCountReducer.class);

            mapReduceWordcountJob.setOutputValueClass(IntWritable.class);

            System.exit(mapReduceWordcountJob.waitForCompletion(true)? 0 : 1);

        } catch (IOException e) {
            e.printStackTrace();

        } catch (InterruptedException e) {
            e.printStackTrace();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();

        }
    }
}
