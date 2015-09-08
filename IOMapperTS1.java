package rmon;

import java.io.IOException;
import java.util.regex.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class IOMapperTS1 extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

  //private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text();
  private String outKey = new String();
  private String outValue = new String();  
  
  @Override
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
		try{
   
      String lineOffset = key.toString(); 
      String str[] = value.toString().split(",");
      outKey = str[0];
      if(str.length >= 8){
        outValue = "ts;" + str[3] + "," + str[1] + "," + str[6] + ","  
                   + str[2] + "," + str[7] + "," + str[4];
      }
      else{
        outValue = "ts;" + str[3] + "," + str[1] + "," + str[6] + ","  
                   + str[2] + "," + "" + "," + str[4];   
      }
      if(str[0].length() <= 12){
        word.set(outKey);
        outVal.set(outValue);
        output.collect(word, outVal); 
      }
      
		}catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}    
  }
}