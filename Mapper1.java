package rmon;

import java.io.IOException;
import java.util.regex.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Mapper1 extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

  //private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text();
  private String outValue = new String();
  private String outKey = new String();
  private String duration = new String();
  
  @Override
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
		try{
      String lineOffset = key.toString(); 
      String str[] = value.toString().split("\t");
      String valueStr[] = str[1].split(",");
      
      // FATP relink (F,T,P => F)
      String patternStr = "\\d{1}[FATP]\\d{2}";
      Pattern pattern = Pattern.compile(patternStr);
      Matcher matcher = pattern.matcher(valueStr[4]);
      boolean matchFound = matcher.find();
      if(matchFound){ // Relink
        valueStr[4] = valueStr[4].substring(0,1) + "F" + valueStr[4].substring(2);
      }
      duration = "";      
      if(valueStr.length > 7){
        duration = valueStr[9];
      }
      else{
        duration = "0";
      }      
      /** relink the d{1}FRd{1} to d{1}F0d{1} **/
      /*if(valueStr[4].substring(2,3).equals("R")){
        valueStr[4] = valueStr[4].substring(0,2) + "0" + valueStr[4].substring(3);
      }*/      
      outKey = valueStr[4] + "," + valueStr[2];        
      /** seq;result:time:sn:duration **/
      if (valueStr.length > 7){
        outValue = valueStr[0] + "#" + valueStr[3] + "}" + valueStr[1] + "}" + str[0] + "}" + duration
                   + "}" + valueStr[8]; // Fail Symptoms      
      }    
      else{
        outValue = valueStr[0] + "#" + valueStr[3] + "}" + valueStr[1] + "}" + str[0] + "}" + duration;
      }
      word.set(outKey);
      outVal.set(outValue);
      output.collect(word, outVal);
		
    }catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}    
  }
}