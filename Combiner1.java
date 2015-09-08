package rmon;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Combiner1 extends MapReduceBase
	implements Reducer<Text, Text, Text, Text> {

  //private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text();
  private String outValue = new String();
  private String outKey = new String();
  
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    HashMap<Integer, String> inputValue = new HashMap<Integer, String>();		
    TreeMap<Integer, String> outputValue = new TreeMap<Integer, String>();
    
    while (values.hasNext()){
      Text value = (Text) values.next();
      String seqTimeResSn[] = value.toString().split("]"); // [0]=seq [1]=result:time:sn      
      int seq =  Integer.parseInt(seqTimeResSn[0]);
      inputValue.put(seq, seqTimeResSn[1]);     
    }   
    outputValue.putAll(inputValue);    
    // Only Store the final result as the consolidated one
    for (Iterator<Integer> iter = outputValue.keySet().iterator(); iter.hasNext();){
      Integer keys = iter.next();
      outValue = outputValue.get(keys);
    }        
    outVal.set(outValue);
    output.collect(key, outVal);    
	}
}