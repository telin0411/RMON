package rmon;

import java.io.IOException;
import java.util.*;
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

public class IOReducer1_Hbase extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    
    private Text word = new Text();
    private Text outVal = new Text();
    private String outValTmp = new String();
    private String outKeyTmp = new String();
    private String outKey = new String();
    public static Configuration config = null;
    public static HTable PandaSN = null;

  public void configure(JobConf conf){
    try{
      config = HBaseConfiguration.create();
      config.set("hbase.zookeeper.quorum", "pshhotdog10,pshhotdog11,pshhotdog12,pshhotdog13,pshhotdog14");
		  config.set("hbase.zookeeper.property.clientPort","2181");
      config.set("hbase.master","172.28.129.58:60010");      
      PandaSN = new HTable(config, "PandaSN"); 
    } catch (IOException e){
      throw new RuntimeException("Failed to create Htable PandaSN", e);
    }
  }

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    /** Specify the initial and eventual I/O object **/
    String inputKey = key.toString();
    outValTmp = "";
    outKeyTmp = "";
    
    /**************************
       Initialize HBase Table
      **************************/
    /*Configuration config = HBaseConfiguration.create(); // Initialize Config
    config.set("hbase.zookeeper.quorum", "pshhotdog10,pshhotdog11,pshhotdog12,pshhotdog13,pshhotdog14");
		config.set("hbase.zookeeper.property.clientPort","2181");
    config.set("hbase.master","172.28.129.58:60010");
    //HBaseAdmin hbase = new HBaseAdmin(config);
    //HTable for Panda information with respect to SN
    HTable PandaSN = new HTable(config, "PandaSN");*/  
		List<String> getListQualifier = new ArrayList<String>();
    try{
      List<Get> queryRowList = new ArrayList<Get>();
      queryRowList.add(new Get(Bytes.toBytes(inputKey)));
      Result[] results = PandaSN.get(queryRowList);
      for (Result r : results) {
     	  for (Cell rowKV : r.rawCells()) {
     	    String quafifierString = new String(CellUtil.cloneQualifier(rowKV));
     	    String pf = new String(CellUtil.cloneFamily(rowKV));
     	    getListQualifier.add(quafifierString);              	
     	  }
      }            
      //System.out.println(inputKey+":"+getListQualifier.get(0)); 
    } catch (IOException e){
      e.printStackTrace();
      System.out.println(e);
    }

    /** Hashmaps for string manipulations **/
    HashMap<String, String> fromTS = new HashMap<String, String>();
    HashMap<String, String> fromMain = new HashMap<String, String>();
    HashMap<String, String> relinkAXI = new HashMap<String, String>();
    HashMap<String, String> relinkAXIC = new HashMap<String, String>();
    HashMap<String, String> FirstIn = new HashMap<String, String>();
    /** For relinking the AXI **/
    String MO = new String();
    String patternStrAXI = "AXI";
    Pattern patternAXI = Pattern.compile(patternStrAXI);      
    String patternStrAXIC = "CHECK";
    Pattern patternAXIC = Pattern.compile(patternStrAXIC);
    
    while (values.hasNext()){
      Text value = (Text) values.next();
      String strIn[] = value.toString().split(";"); // [0]=main/ts indicator, [1]=info
      /** Information from main table **/
      if(strIn[0].equals("main")){
        String str[] = strIn[1].split(",");
        outValTmp = str[0] + "," + str[1] + "," + str[2] + "," + str[3] + ","
                     + str[4] + "," + str[5] + "," + str[6] + "," + str[7]; 
        outKeyTmp = str[1] + "," + str[6] + "," + str[3]; // time,workID,result 
        MO = str[7];
        /** AXI/AXI CHECK **/
        Matcher matcherAXI = patternAXI.matcher(str[2]);
        boolean matchFoundAXI = matcherAXI.find();    
        Matcher matcherAXIC = patternAXIC.matcher(str[2]);
        boolean matchFoundAXIC = matcherAXIC.find();
        /** AXI relink : the line of the sn is defined as the line it appeared first 
            if  only AXI detected, simply ignore the process
            if AXI CHECK detected, and series of this sn only contain AXI/AXI CHECK
            the line would be relinked with respect to the line of AXI CHECK
            if  other grpnm detected, the line of the first appearance serves all **/
        if(matchFoundAXI && matchFoundAXIC){
          relinkAXIC.put(inputKey+","+MO, str[1]+":"+str[4]);
        }
        if(!matchFoundAXI && !relinkAXI.containsKey(inputKey+","+MO) && !str[4].substring(2,3).equals("R")){
          relinkAXI.put(inputKey+","+MO, str[1]+":"+str[4]);
        }         
        else if(!matchFoundAXI && relinkAXI.containsKey(inputKey+","+MO) && str[1].compareTo(relinkAXI.get(inputKey+","+MO).split(":")[0]) < 0 && !str[4].substring(2,3).equals("R")){
          relinkAXI.put(inputKey+","+MO, str[1]+":"+str[4]);
        }       
        fromMain.put(outKeyTmp+";"+str[0], outValTmp);
      }
      /** Filter out the sn if it already existed in the First_In Table.
          Comparison is done under the condition of the same test station.
          If the time recorded in the table is <= the time recorded in the 
          main table, this sn would be completely discarded 
      **/
      else if(strIn[0].equals("FI")){
        String str[] = strIn[1].split(",");
        FirstIn.put(str[1]+str[3], str[0]); // (K,V)=(grpnm+sn,timestamp)       
      }
      /** Information from TS table **/
      else if(strIn[0].equals("ts")){
        String str[] = strIn[1].split(",");
        outValTmp = str[3] + "," + str[4] + "," + str[5]; 
        outKeyTmp = str[0] + "," + str[1] + "," + str[2];      
        if (fromTS.containsKey(outKeyTmp)){
          String tstmp = new String();
          tstmp = fromTS.get(outKeyTmp);          
          fromTS.put(outKeyTmp, outValTmp+";"+tstmp);
        }
        else
          fromTS.put(outKeyTmp, outValTmp);
      }                                    
    }
    
    Iterator it = fromMain.entrySet().iterator();
    while (it.hasNext()) {
      outValTmp = "";
      Map.Entry pairs = (Map.Entry) it.next();      
      outKey = inputKey;
      word.set(outKey);      
      String str[] = pairs.getValue().toString().split(",");
      Matcher matcherAXIOut = patternAXI.matcher(str[2]);
      boolean matchFoundAXIOut = matcherAXIOut.find();
      /** Relinking procedure **/    
      MO = str[7];  
      if (relinkAXI.get(inputKey+","+MO)==null && relinkAXIC.get(inputKey+","+MO)!=null)
        str[4] = relinkAXIC.get(inputKey+","+MO).split(":")[1];  
      else if (relinkAXI.get(inputKey+","+MO)!=null && !str[4].substring(1,2).equals("D"))
        str[4] = relinkAXI.get(inputKey+","+MO).split(":")[1];        
      
      outValTmp = str[0] + "," + str[1].substring(4) + "," + str[2] + "," + str[3] + ","
                  + str[4] + "," + str[5] + "," + str[6];
                            
      String hashTSkey = pairs.getKey().toString().split(";")[0];
      if(fromTS.containsKey(hashTSkey)){        
        String outtstmp[] = fromTS.get(hashTSkey).split(";");
        outValTmp = outValTmp + "," + outtstmp[0];
        if (outtstmp.length > 1)
          fromTS.put(hashTSkey, outtstmp[1]+";");
      }
      /*** filter out the duplicated devices due to its "first in" appearance is made ***/
      if(FirstIn.containsKey(str[2]+inputKey)){
        if(FirstIn.get(str[2]+inputKey).compareTo(str[1]) >= 0){
          outVal.set(outValTmp);
          output.collect(word, outVal);
        }
      }
      else if (str[4].substring(2,3).equals("R")){
        //do nothing
      }
      else{
        outVal.set(outValTmp);
        output.collect(word, outVal);
      }
      it.remove(); // avoids a ConcurrentModificationException
    }

	}
}
