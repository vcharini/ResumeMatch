package edu.tamu.isys.resumematch;

import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Arrays.asList;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;


public class Map2 extends Mapper<Object, BSONObject, Text, Text> {

	HashMap<String, Map<String, Integer>> keyWordFreq;
	MongoClient mongoClient1;
	MongoDatabase db;
	
	/*
	 * Mapper takes in all the positions whose deadline is today and generates the product of the weightage and the frequency of 
	 * occurrence for every keyWord weightage - resume combination specified for every position. It sends keyWord:resumeNumber
	 * as key and the corresponding product as value.
	 */

	public Map2(){
		/*
		 * Takes data from keyWords collection with the frequency of occurrence of the keyWord
		 */
		mongoClient1 = new MongoClient();
		db = mongoClient1.getDatabase("test");
		
		FindIterable<Document> iterable = db.getCollection("keyWords").find();
		keyWordFreq = new HashMap<String,Map<String, Integer>>();
		
		for(Document keyWord: iterable){
			HashMap<String,Integer> resumeValues = new HashMap<String,Integer>();
			System.out.println(keyWord);
			Document d=(Document)keyWord.get("Resumes");
			for(String resumeNumber: d.keySet()){
				resumeValues.put(resumeNumber, Integer.valueOf(d.get(resumeNumber).toString()));
			}
			keyWordFreq.put(keyWord.getString("_id"), resumeValues);
		}
	}
	
	@Override
	public void map(Object key, BSONObject value, Mapper.Context context) throws IOException, InterruptedException {
		
		/*
		 * Calculating product of keyWordWeightage and frequency
		 */
		
		Date dNow = new Date();
	    SimpleDateFormat ft = new SimpleDateFormat("yyyy.dd.MM");
		
	    if(value.get("deadline").toString().equals(ft.format(dNow).toString())){
			BasicDBObject weightage = (BasicDBObject)value.get("keyWeightage");
			for(String keyWord: weightage.keySet()){
				if(keyWordFreq.size()>0 && keyWordFreq.containsKey(keyWord)){
				for(String resumeAndFrequency: keyWordFreq.get(keyWord).keySet()){
					Double keyWeightage=Double.parseDouble(weightage.get(keyWord).toString());
					Integer frequency=keyWordFreq.get(keyWord).get(resumeAndFrequency);
					Double product =keyWeightage*frequency;
					context.write(new Text(key.toString()+":"+resumeAndFrequency), new Text(product.toString()));
				}
				}
			}
		}
	}

}