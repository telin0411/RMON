package rmon;

import java.io.IOException;
import java.util.*;
import java.lang.Math.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Reducer1 extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    
  /** Create a data structure for the hash-like function in perl with respect to SN **/
  class calUPH {
    private String firstPassTime = "99999999999999";
    private int avgUPH;
    private int maxUPH;
    private HashMap<Integer, Integer> UPH = new HashMap<Integer, Integer>();
    private HashMap<String, Integer> firstAppear = new HashMap<String, Integer>();
    private String hrLog = "";
             
    public void calculateUPH(){
      int maxUPHTmp = 0;
      int avgUPHTmp = 0;
      int UPHcnt = 0;        
      Iterator it = UPH.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry pairs = (Map.Entry) it.next();
        int uphTmp = Integer.parseInt(pairs.getValue().toString());
        avgUPHTmp += uphTmp;
        if(uphTmp >= maxUPHTmp){
          maxUPHTmp = uphTmp;
        }        
        UPHcnt++;
        hrLog = hrLog + pairs.getKey().toString() + "(" + uphTmp + ")" + "=>";
        it.remove(); // avoids a ConcurrentModificationException
      }
      hrLog += "LAST";
      //hrLog = "AVG=" + avgUPHTmp + " / CNT=" + UPHcnt; 
      if (UPHcnt > 0){
        this.avgUPH = avgUPHTmp / UPHcnt;
      }
      else{
        this.avgUPH = 0;
      }
      this.maxUPH = maxUPHTmp;      
    }    
    public void addUPH(String sn, String inputTime, String result){
      int inputHour = Integer.parseInt(inputTime.substring(4, 6));
      //int inputHour = Integer.parseInt(inputTime.substring(8, 10));
      if(firstAppear.containsKey(sn) && inputHour < firstAppear.get(sn) && result.equals("P")){
        int originUPH = UPH.get(firstAppear.get(sn));
        originUPH--;
        if(originUPH <= 0){
          UPH.remove(firstAppear.get(sn));
        }
        else{
          UPH.put(firstAppear.get(sn), originUPH);
        }
        firstAppear.put(sn, inputHour);
        if(UPH.containsKey(inputHour)){
          int newUPH = UPH.get(inputHour);
          newUPH++;
          UPH.put(inputHour, newUPH);
        }
        else{
          UPH.put(inputHour, 1);
        }
      }
      else if(!firstAppear.containsKey(sn) && result.equals("P")){
        firstAppear.put(sn, inputHour);
        if(UPH.containsKey(inputHour)){
          int firstUPH = UPH.get(inputHour);
          firstUPH++;
          UPH.put(inputHour, firstUPH);
        }
        else{
          UPH.put(inputHour, 1);
        }        
      }    
    }        
    public int getAvgUPH() {return this.avgUPH;}
    public int getMaxUPH() {return this.maxUPH;}
    public String getUPHLog() {return this.hrLog;}
  }    
  /** Basic Statistical analysis **/
  class statistics_func{
    private double stdevCTPass, stdevCTFail;
    private double avgCTPass, avgCTFail;
    private double maxCTPass=0, minCTPass=99999, maxCTFail=0, minCTFail=99999;
    private HashMap<String, Double> durationPass = new HashMap<String, Double>();
    private HashMap<String, Double> durationFail = new HashMap<String, Double>();
    public void calStdDev(){
      double tmp = 0;
      Iterator itP = durationPass.entrySet().iterator();
      while (itP.hasNext()) {
        Map.Entry pairs = (Map.Entry) itP.next();  
        double currentP = Double.parseDouble(pairs.getValue().toString());
        tmp += (avgCTPass-currentP) * (avgCTPass-currentP);      
      }  
      stdevCTPass = Math.sqrt(tmp);
      tmp = 0;    
      Iterator itF = durationFail.entrySet().iterator();
      while (itF.hasNext()) {
        Map.Entry pairs = (Map.Entry) itF.next();    
        double currentF = Double.parseDouble(pairs.getValue().toString());
        tmp += (avgCTFail-currentF) * (avgCTFail-currentF);      
      }                  
      stdevCTFail = Math.sqrt(tmp);      
    }
    public void calAvgCT(){
      double tmp=0, cnt=0;
      //maxCTPass=0; maxCTFail=0;
      //minCTPass=99999; minCTFail=99999;
      Iterator itP = durationPass.entrySet().iterator();
      while (itP.hasNext()) {
        Map.Entry pairs = (Map.Entry) itP.next();  
        double currentP = Double.parseDouble(pairs.getValue().toString());
        /*if(currentP > maxCTPass)
          maxCTPass = currentP;
        if (currentP < minCTPass)
          minCTPass = currentP;*/
        tmp += currentP;
        cnt++;      
      }  
      avgCTPass = tmp / cnt;
      tmp = 0; cnt=0;    
      Iterator itF = durationFail.entrySet().iterator();
      while (itF.hasNext()) {
        Map.Entry pairs = (Map.Entry) itF.next();    
        double currentF = Double.parseDouble(pairs.getValue().toString());
        /*if(currentF > maxCTFail)
          maxCTFail = currentF;
        if (currentF < minCTFail)
          minCTFail = currentF;*/ 
        tmp += currentF; 
        cnt++;     
      }                  
      avgCTFail = tmp / cnt;      
    }    
    public void addDurationPass(String sn, double duration){
      durationPass.put(sn, duration);
      if(duration > maxCTPass)
        maxCTPass = duration;
      if(duration < minCTPass)
        minCTPass = duration;
    }
    public void addDurationFail(String sn, double duration){
      durationFail.put(sn, duration);
      if(duration > maxCTFail)
        maxCTFail = duration;
      if(duration < minCTFail)
        minCTFail = duration; 
    }   
    public double getStdevPass() {return this.stdevCTPass;}
    public double getStdevFail() {return this.stdevCTFail;}
    public double getAvgCTPass() {return this.avgCTPass;}
    public double getAvgCTFail() {return this.avgCTFail;}    
    public double getMaxCTPass() {return this.maxCTPass;}
    public double getMaxCTFail() {return this.maxCTFail;} 
    public double getMinCTPass() {return this.minCTPass;}
    public double getMinCTFail() {return this.minCTFail;}         
  }
    
    /** Required declarations for inputs **/
    private Text outKey = new Text();
    private Text outVal = new Text();
    private String outValue = new String();
    private String inputFail = new String();
    private String result = new String();
    private String time = new String();
    private String sn = new String();
    private String timeStamp = new String();
    private String inputKey = new String();
    private String testStr = new String();
    /** Required declarations for outputs/calculations **/
    private int MAXUPH, AVGUPH, seq;
    private double duration, MAX_CT_PASS, MIN_CT_PASS, AVG_CT_PASS, AVG_CT_FAIL, MAX_CT_FAIL, MIN_CT_FAIL;
    private double SDV_CT_PASS, SDV_CT_FAIL;  // standard deviations for CT
 
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
    int inputCount = 0;
    int fail = 0;
    double durPTmp = 0, durFTmp = 0;
    /** The below hashmap is for consolidating the results of a device under the same grpnm
        into its last appeared result during the series **/
    HashMap<String, String> consolidate = new HashMap<String, String>();
    // Initiations
    duration = 0;
    calUPH uph = new calUPH(); // objects for calculating UPH
    statistics_func stats_func = new statistics_func();
    timeStamp = "";
    testStr = "";
    inputKey = key.toString();
    MAX_CT_PASS = 0;
    MAX_CT_FAIL = 0;
    MIN_CT_PASS = 0;
    MIN_CT_FAIL = 0;
    
    /** Receiving values from the mapper **/
    while (values.hasNext()){
      /** Input strings manipulation **/
      Text value = (Text) values.next();
      String seqTimeRes[] = value.toString().split("#"); // [0]=seq [1]=result:time:sn:duration
      seq = Integer.parseInt(seqTimeRes[0]); // sequence number in int form
      String timeRes[] = seqTimeRes[1].toString().split("}"); // [0]=result, [1]=time, [2]=sn, [3]=duration
      result = timeRes[0];
      time = timeRes[1];
      sn = timeRes[2];
      duration = Double.parseDouble(timeRes[3]);
      
      /** Consolidating the results as the final one occured within the same grpnm/timestamp **/
      if(!consolidate.containsKey(sn) && result.equals("P")){
        //consolidate.put(sn, result+";"+time+","+seq);
        stats_func.addDurationPass(sn, duration); // add to the hashset for stdev_pass calculations
        consolidate.put(sn, result+";"+time+","+seq+";"+duration);
        inputCount++;
      }
      else if(!consolidate.containsKey(sn) && result.equals("F")){
        //consolidate.put(sn, result+";"+time+","+seq);
        stats_func.addDurationFail(sn, duration); // add to the hashset for stdev_pass calculations
        consolidate.put(sn, result+";"+time+","+seq+";"+duration);
        inputCount++;
        fail++;
      }
      /** parameters to compare who comes as the last sn **/
      String pastTime = new String();
      String pastSeqs = new String();
      pastTime = consolidate.get(sn).split(";")[1].split(",")[0];
      pastSeqs = consolidate.get(sn).split(";")[1].split(",")[1];      
      double pastSeq = Double.parseDouble(pastTime+pastSeqs);
      double currentSeq = Double.parseDouble(time+seq);
      /** Dealing with passes **/
      if (result.equals("P")){
      //if(result.equals("P") && (time+","+seq).compareTo(consolidate.get(sn).split(";")[1]) > 0){
        //testStr += ("("+sn+","+time+seq+","+result+")");
        stats_func.addDurationPass(sn, duration); // add to the hashset for stdev_pass calculations
        if(currentSeq > pastSeq){        
          if(consolidate.get(sn).split(";")[0].equals("F")){
            fail--;
          }
          //consolidate.put(sn, result+";"+time+","+seq); //store new results       
          consolidate.put(sn, result+";"+time+","+seq+";"+duration);
        }
      }
      /** Dealing with Failures **/
      else if (result.equals("F")){
      //else if (result.equals("F") && (time+","+seq).compareTo(consolidate.get(sn).split(";")[1]) > 0){
        //testStr += ("("+sn+","+time+seq+","+result+")");
        stats_func.addDurationFail(sn, duration); // add to the hashset for stdev_fail calculations
        if(currentSeq > pastSeq){
          fail++;
          if(consolidate.get(sn).split(";")[0].equals("F")){
            fail--;
          }
          //consolidate.put(sn, result+";"+time+","+seq); // store new results       
          consolidate.put(sn, result+";"+time+","+seq+";"+duration);
        }
      }     
      //timeStamp = timeStamp + timeRes[1] + "(" + timeRes[2] + "," + result + ")=>"; // timestamps recording
      uph.addUPH(sn, time, result); // Insert the required information for recording UPH        
    }   
    
    /** Print out the input SN for debugging **/
    if(inputKey.equals("3F01,AXI")){
      // sorted order with respect to sn
      testStr = "\n";
      String toTest = new String();
      SortedSet<String> SNs = new TreeSet<String>(consolidate.keySet());
      for (String snIn : SNs) { 
        String SNINFO = consolidate.get(snIn);
        String valueSN[] = SNINFO.split(";")[1].split(",");
        toTest = "2014" + valueSN[0] + "," + snIn + "," + SNINFO.split(";")[0] + "," +SNINFO.split(";")[2];
        //toTest = valueSN[0] + "," + snIn + "," + SNINFO.split(";")[0] + "," +SNINFO.split(";")[2];
        testStr += (toTest+"\n");
      }   
    }
    
    /** UPH calculations **/
    uph.calculateUPH();
    AVGUPH = uph.getAvgUPH();
    MAXUPH = uph.getMaxUPH();
    /** Cycle time calculations **/
    stats_func.calAvgCT();
    stats_func.calStdDev();
    AVG_CT_PASS = stats_func.getAvgCTPass(); 
    AVG_CT_FAIL = stats_func.getAvgCTFail();
    SDV_CT_PASS = stats_func.getStdevPass();
    SDV_CT_FAIL = stats_func.getStdevFail();
    MAX_CT_PASS = stats_func.getMaxCTPass();
    MAX_CT_FAIL = stats_func.getMaxCTFail();
    MIN_CT_PASS = stats_func.getMinCTPass();
    MIN_CT_FAIL = stats_func.getMinCTFail();
        
    /** Prinint the outputs **/
    timeStamp += "LAST";
    inputFail = inputCount + "," + fail;
    //outValue = inputFail + "[" + timeStamp + "]" + " MAXUPH=" + MAXUPH 
    //             + " AVGUPH = " + AVGUPH + " Log: " + uph.getUPHLog();
    outValue = inputFail + ",MAXUPH=" + MAXUPH + ",AVGUPH=" + AVGUPH
               + ",MAX_CT_PASS=" + MAX_CT_PASS + ",MAX_CT_FAIL=" + MAX_CT_FAIL;
               //+ " AVG_CT_PASS=" + AVG_CT_PASS + " StdDev_CT_Pass=" + SDV_CT_PASS;
               //+ "\nTime,SN,Detail" + testStr + "\nEND\n";
               //+ " UPHLog: " + uph.getUPHLog();

    outVal.set(outValue);
    outKey.set(inputKey);
    output.collect(outKey, outVal);
	}
}
