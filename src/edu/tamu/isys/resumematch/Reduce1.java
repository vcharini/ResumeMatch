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
//import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import static java.util.Arrays.asList;


public class Reduce1 extends Reducer<Text, Text, Text, Text> {
	
	/*
	 * Reducer function finds the frequency of the keyWords from the resumeNumbers received for every keyWord and then saves it 
	 * to MongoDB in keyWords collection with every document containing the keyWord and the corresponding frequency of occurrence
	 * in every resume.
	 */
	
	@Override			
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		HashMap<String,Integer> resumeAndFrequency = new HashMap<String,Integer>();
		MongoClient mongoClient1;
		MongoDatabase db;
		mongoClient1 = new MongoClient();
		db = mongoClient1.getDatabase("test");
		
		/*
		 * Creating a map to find the total sum 
		 */
		for(Text resumeNumber: values){
			
			
			if(!resumeAndFrequency.isEmpty() && resumeAndFrequency.containsKey(resumeNumber.toString()))
				resumeAndFrequency.put(resumeNumber.toString(),resumeAndFrequency.get(resumeNumber.toString())+1);
			else
				resumeAndFrequency.put(resumeNumber.toString(),1);
		}
		
		/*
		 * Building and inserting document in keyWords collection in MongoDB 
		 */
		FindIterable<Document> cursor = db.getCollection("keyWords").find(new BasicDBObject("_id", key.toString()));
		if(!(cursor.first()!=null)){
				
			Document doc = new Document();
			doc.append("_id", key.toString());
			Document subDocument = new Document();
			for(String resumeNo: resumeAndFrequency.keySet()){
				subDocument.append(resumeNo,resumeAndFrequency.get(resumeNo));
			}
			doc.append("Resumes",subDocument);
			db.getCollection("keyWords").insertOne(doc);
		}
		else{
			Document doc=(Document)cursor.first();
			Document subDocument = new Document();
			for(String resumeNo: resumeAndFrequency.keySet()){
				subDocument.append(resumeNo,resumeAndFrequency.get(resumeNo));
			}
			doc.append("Resumes", subDocument);
		}
}
}

