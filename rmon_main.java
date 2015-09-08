package rmon;

import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.*;
//import org.apache.pig.EvalFunc;

import org.apache.hadoop.conf.*;
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

public class rmon_main {

	public static void main(String[] args) throws Exception {
	     long exeTime = 0;

       /** Main/TS Tables Joining Job **/       
       JobConf conf = new JobConf(rmon_main.class);
       conf.setJobName("Albert_RMON");		     
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
       
       //Configuration config = HBaseConfiguration.create();
       //conf.set("hbase.master","172.28.129.58:60010");
       //DistributedCache.addCacheFile(new URI("/myapp/lookup.dat#lookup.dat"), conf);
       
       MultipleInputs.addInputPath(conf, 
                                   new Path(args[0]),
                                   TextInputFormat.class,
                                   IOMapper1.class);
	     MultipleInputs.addInputPath(conf, 
                                   new Path(args[1]),
                                   TextInputFormat.class,
                                   IOMapperTS1.class);                      
	     MultipleInputs.addInputPath(conf, 
                                   new Path(args[3]),
                                   TextInputFormat.class,
                                   IOMapperFI.class); 
                                                                               
       //conf.setMapperClass(IOMapper.class);      
	     conf.setReducerClass(IOReducer1_Hbase.class);
	     conf.setOutputKeyComparatorClass(KeyComparator.class); 	     
	     //conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     /** Number of Reducer Tasks **/
	     conf.setNumReduceTasks(5);           
	     //FileInputFormat.setInputPaths(conf, new Path(args[0]));    
	     FileOutputFormat.setOutputPath(conf, new Path(args[2]));	     
       long start =  new Date().getTime();       
       /** Run Job **/
	     JobClient.runJob(conf);
       //boolean status = conf.waitForCompletion(true);
       /** Time Slots **/
       long end = new Date().getTime();
       System.out.println("*** The IO Job took "+((end-start) /1000)+ " seconds"); 
       exeTime = exeTime + (end-start) / 1000;
       
       /** Statistics and analysis Job **/
       JobConf conf1 = new JobConf(rmon_main.class);
	     conf1.setJobName("ALbert_RMON_1");
	     conf1.setOutputKeyClass(Text.class);
	     conf1.setOutputValueClass(Text.class);         
       conf1.setMapperClass(Mapper1.class); 
	     conf1.setReducerClass(Reducer1_2.class);
	     conf1.setOutputKeyComparatorClass(KeyComparator.class);	     
       conf1.setInputFormat(TextInputFormat.class);
	     conf1.setOutputFormat(TextOutputFormat.class);
	     /** Number of Reducer Tasks **/
       //conf1.setNumReduceTasks(5);           
	     FileInputFormat.setInputPaths(conf1, new Path(args[2]+"/*")); 
       FileOutputFormat.setOutputPath(conf1, new Path(args[2]+"_2"));
       start =  new Date().getTime(); 
	     /** Run Job **/
       JobClient.runJob(conf1);              
       /** Time Slots **/
       end = new Date().getTime();
       System.out.println("*** The Calculating Job took "+((end-start) /1000)+ " seconds"); 
       exeTime = exeTime + (end-start) / 1000; 
       
       /** OutPut the total execution time record **/
       System.out.println("-------------------------------------------------------------");
       System.out.println("*** Total Execution Time: "+exeTime+" seconds");  
       System.out.println("-------------------------------------------------------------");
	}
}
