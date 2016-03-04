package edu.tamu.isys.resumematch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;

import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;

public class Map1 extends Mapper<LongWritable, Text, Text, Text> {

	HashMap<String,Integer> wordSet = new HashMap<String,Integer>();
		
	/*
	 * Mapper searches for the keyWords in every resume line read from the text file in HDFS and sends the keyWord and
	 * the resumeNumber to reducer in order to calculate the frequency of occurrence of every keyWord in every resume.
	 */
	
	public Map1(){
		
		/*
		 * Extracting the keyWords from a text file to use for scanning resume to be used for the mapper phase.
		 */
		InputStream is = null;
		
		try{
		File fileName=new File("C:/keyWords.pdf");
	    is = new FileInputStream(fileName);
	    ContentHandler contenthandler = new BodyContentHandler();
	    Metadata metadata = new Metadata();
	    PDFParser pdfparser = new PDFParser();
	    
	    pdfparser.parse(is, contenthandler, metadata, new ParseContext());
	    String keyWordList=contenthandler.toString();
	    String[] values=keyWordList.split("::");
	    for(int i=0;i<values.length;i++)
		wordSet.put(values[i],1);
	    is.close();
		}
		catch (Exception e) {
		      e.printStackTrace();
		}
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		/*
		 *  Splitting the string with ::: as delimiter to get resumeNumber and the parsed resume.
		 */
		String line = value.toString();                                                          
		String resumeNumber = line.split(":::")[0];
		String[] split_values = line.split(":::")[1].split(" ");								
		for(Integer i=0; i<split_values.length;i++){											
			
				/*
				 * Iterating through the resume to identify the keyWords and adding it to context write.
				 */
				if(wordSet.containsKey(split_values[i]))
					context.write(new Text(split_values[i]),new Text(resumeNumber));
				else if(i+1<split_values.length && wordSet.containsKey(split_values[i]+" "+split_values[i+1]))
					context.write(new Text(split_values[i]+" "+split_values[i+1]),new Text(resumeNumber));
				else if(i+2<split_values.length && wordSet.containsKey(split_values[i]+" "+split_values[i+1]+" "+split_values[i+2]))
					context.write(new Text(split_values[i]+" "+split_values[i+1]+" "+split_values[i+2]),new Text(resumeNumber));
		}
	}
}