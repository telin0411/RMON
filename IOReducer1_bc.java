package rmon;

import java.io.IOException;
import java.util.*;
import java.util.regex.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class IOReducer1_bc extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    
    private Text word = new Text();
    private Text outVal = new Text();
    private String outValTmp = new String();
    private String outKeyTmp = new String();
    private String outKey = new String();
    
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
    String inputKey = key.toString();
    outValTmp = "";
    outKeyTmp = "";

    /** Hashmaps for string manipulations **/
    HashMap<String, String> fromTS = new HashMap<String, String>();
    HashMap<String, String> fromMain = new HashMap<String, String>();
    HashMap<String, String> relinkAXI = new HashMap<String, String>();
    HashMap<String, String> relinkAXIC = new HashMap<String, String>();
    HashMap<String, String> FirstIn = new HashMap<String, String>();
    /** For relinking the AXI **/
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
                     + str[4] + "," + str[5] + "," + str[6]; 
        outKeyTmp = str[1] + "," + str[6] + "," + str[3]; // time,workID,result 
        //outKeyTmp = str[1] + "," + str[2] + "," + str[3] + "," + str[6]; // time,grpnm,result,workID 
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
          relinkAXIC.put(inputKey, str[1]+":"+str[4]);
        }          
        //if(matchFoundAXIC && !relinkAXI.containsKey(inputKey) && !str[4].substring(2,3).equals("R")){
        //  relinkAXI.put(inputKey, str[1]+":"+str[4]);
        //}         
        if(!matchFoundAXI && !relinkAXI.containsKey(inputKey) && !str[4].substring(2,3).equals("R")){
          relinkAXI.put(inputKey, str[1]+":"+str[4]);
        }         
        else if(!matchFoundAXI && relinkAXI.containsKey(inputKey) && str[1].compareTo(relinkAXI.get(inputKey).split(":")[0]) < 0 && !str[4].substring(2,3).equals("R")){
          relinkAXI.put(inputKey, str[1]+":"+str[4]);
        }       
        fromMain.put(outKeyTmp+";"+str[0], outValTmp);
        //fromMain.put(outKeyTmp, outValTmp);
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
          //fromTS.put(outKeyTmp, outValTmp);
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
      //if(matchFoundAXIOut){
        //str[4] = relinkAXI.get("LINE");
      
      //if (relinkAXI.get(inputKey)!=null && str[4].substring(2,3).equals("R") && str[1].compareTo(relinkAXI.get(inputKey).split(":")[0]) < 0)   
      //else if (relinkAXI.get(inputKey)!=null && str[4].substring(2,3).equals("R") && str[1].compareTo(relinkAXI.get(inputKey).split(":")[0]) > 0)             
      if (relinkAXI.get(inputKey)==null && relinkAXIC.get(inputKey)!=null)
        str[4] = relinkAXIC.get(inputKey).split(":")[1];  
      else if (relinkAXI.get(inputKey)!=null && !str[4].substring(1,2).equals("D"))
        str[4] = relinkAXI.get(inputKey).split(":")[1];        
      
      //else if (relinkAXI.get(inputKey)!=null && str[4].substring(2,3).equals("R"))
      //  str[4] = relinkAXI.get(inputKey).split(":")[1];
      //}
      outValTmp = str[0] + "," + str[1].substring(4) + "," + str[2] + "," + str[3] + ","
                  + str[4] + "," + str[5] + "," + str[6];
      //if(fromTS.containsKey(pairs.getKey().toString())){
      //  outValTmp = outValTmp + "," + fromTS.get(pairs.getKey().toString());
      //}
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
