package edu.tamu.isys.resumematch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import java.net.UnknownHostException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

public class mapReduce2 extends MongoTool {
		
		/*
		 * This mapreduce function is executed to match the position requirements and the resumes available in the database. This 
		 * mapreduce takes data from MongoDB and updates MongoDB at the end of reduce and hence uses MongoTool to connect to MongoDB.
		 */
		
	    public mapReduce2() throws UnknownHostException {
	        
	    	/*
	    	 * Configuration of the mapreduce job using MongoConfigTool. The mapper, reducer, input and output type is set. The
	    	 * input and output collection in MongoDB which will be accessed is also configured.
	    	 */
	    	
	    	setConf(new Configuration());

	        if (MongoTool.isMapRedV1()) {
	            MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
	            MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);
	        } else {
	            MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
	            MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
	        }
	        
	        MongoConfigUtil.setInputURI(getConf(), "mongodb://localhost:27017/test.jobPositions");
	        MongoConfigUtil.setOutputURI(getConf(), "mongodb://localhost:27017/test.jobPositions");

	        MongoConfigUtil.setMapper(getConf(), Map2.class);
	        MongoConfigUtil.setReducer(getConf(), Reduce2.class);
	        MongoConfigUtil.setMapperOutputKey(getConf(), Text.class);
	        MongoConfigUtil.setMapperOutputValue(getConf(), Text.class);
	        MongoConfigUtil.setOutputKey(getConf(), IntWritable.class);
	        MongoConfigUtil.setOutputValue(getConf(), BSONWritable.class);
	       
	        
	    }

	    public static void main(final String[] pArgs) throws Exception {
	        System.exit(ToolRunner.run(new mapReduce2(), pArgs));
	    }
}
