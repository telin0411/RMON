package rmon;

import java.io.IOException;
import java.util.regex.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class IOMapper1 extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

  //private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text();
  private String outKey = new String();
  private String outValue = new String();  
  
  /*private HTable PandaSN;
  private HBaseConfiguration HBconf;
  public void configure(JobConf job){
    HBconf = new HBaseConfiguration();
    try{      
      PandaSN = new HTable(HBconf, "PandaSN"); 
    } catch (IOException e){
      throw new RuntimeException("Failed to create Htable PandaSN", e);
    }
  }*/ 
  
  @Override
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
		try{
      String lineOffset = key.toString(); 
      String str[] = value.toString().split(",");
      outKey = str[0];
      
      //String patternStr = "\\w+\\_R$";
      String patternStr = "\\_R$";
      Pattern pattern = Pattern.compile(patternStr);
      Matcher matcher = pattern.matcher(str[8]);
      boolean matchFound = matcher.find(); // get rid off the _R inputs
      if (!matchFound){  
        outValue = "main;" + str[1] + "," + str[2] + "," + str[8] + "," + str[5] + ","
                   + str[3] + "," + str[6] + "," + str[4] + "," + str[7];    
        if(str[0].length() <= 12 && !str[3].substring(2,3).equals("R")){               
          word.set(outKey);
          outVal.set(outValue);
          output.collect(word, outVal);
        }
      } 
         
		}catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}    
  }
}