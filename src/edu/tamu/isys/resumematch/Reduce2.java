package edu.tamu.isys.resumematch;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static java.util.Arrays.asList;

import com.mongodb.hadoop.io.MongoUpdateWritable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

public class Reduce2 extends Reducer<Text, Text, NullWritable, MongoUpdateWritable> {
		
		/*
		 * Reducer sums the product of key weightage and frequency for every resume job position wise and appends
		 * it to the existing position in jobPositions collection as a sub document.
		 */
	
		private MongoUpdateWritable reduceResult;	
		
		public Reduce2(){
		        reduceResult = new MongoUpdateWritable();
		}
		
		@Override			
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			/*
			 * Finding sum and inserting the sub document built using MongoUpdateWritable.
			 */
			
			double sum=0.0;
			for(Text weightedFrequency: values){
				sum+=Double.parseDouble(weightedFrequency.toString());
			}
			
			BasicBSONObject query = new BasicBSONObject("_id", new ObjectId(key.toString().split(":")[0]));
			  BasicBSONObject valueElements = new BasicBSONObject();
		      valueElements.put("Resumes", new BasicBSONObject(key.toString().split(":")[1],sum));
		      BasicBSONObject update = new BasicBSONObject("$push", valueElements);
		      reduceResult.setQuery(query);
		      reduceResult.setModifiers(update);
		      
		      context.write(null, reduceResult);
		}
}	
