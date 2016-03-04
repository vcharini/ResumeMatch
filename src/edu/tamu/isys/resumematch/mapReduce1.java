package edu.tamu.isys.resumematch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import java.net.UnknownHostException;
import org.apache.hadoop.io.IntWritable;

public class mapReduce1 {
	
	public static void main(String[] args) throws Exception{	
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "resumeKeyWordFrequency");
		job.setJarByClass(mapReduce1.class);                 		//Set the Jar by finding where a given class came from
		job.setMapperClass(Map1.class); 					  		//Set the Mapper class for this job
		job.setReducerClass(Reduce1.class); 						//Set the Reducer class for this job
		job.setOutputKeyClass(Text.class); 				  			//Set the output key type
		job.setOutputValueClass(Text.class); 			  			//Set the output value type
		job.setInputFormatClass(TextInputFormat.class);	  			//Set the InputFormat for the job
		job.setOutputFormatClass(TextOutputFormat.class); 			//Set the OutputFormat for the job 
	
		FileInputFormat.addInputPath(job, new Path(args[0]));		//To add path to the input for the map-reduce job
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 	//To set the path of the output directory for the map-reduce job
		
		job.waitForCompletion(true);
	}	
}
