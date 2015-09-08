package rmon;

import java.io.IOException;
import java.util.*;
import java.text.*;
import java.lang.Math.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Reducer1_2 extends MapReduceBase
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
    private double stdevCTPass=0.00, stdevCTFail=0.00;
    private double avgCTPass=0.00, avgCTFail=0.00;
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
      Iterator itP = durationPass.entrySet().iterator();
      while (itP.hasNext()) {
        Map.Entry pairs = (Map.Entry) itP.next();  
        double currentP = Double.parseDouble(pairs.getValue().toString());
        tmp += currentP;
        cnt++;      
      }  
      if (tmp != 0)                  
        avgCTPass = tmp / cnt;  
      else
        avgCTPass = 0.00;
      tmp = 0; cnt=0;    
      Iterator itF = durationFail.entrySet().iterator();
      while (itF.hasNext()) {
        Map.Entry pairs = (Map.Entry) itF.next();    
        double currentF = Double.parseDouble(pairs.getValue().toString());
        tmp += currentF; 
        cnt++;     
      }
      if (tmp != 0)                  
        avgCTFail = tmp / cnt;  
      else
        avgCTFail = 0.00;     
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
    public double getMinCTPass() {
      if (this.minCTPass == 99999)
        return 0;
      else
        return this.minCTPass;
    }
    public double getMinCTFail() {
      if (this.minCTFail == 99999)
        return 0;
      else  
        return this.minCTFail;
    }         
  }
    
    /** Required declarations for inputs **/
    private Text outKey = new Text();
    private Text outVal = new Text();
    private String outValue = new String();
    private String inputFail = new String();
    private String result = new String();
    private String time = new String();
    private String sn = new String();
    private String failSymp = new String();
    private String timeStamp = new String();
    private String inputKey = new String();
    private String testStr = new String();
    /** Required declarations for outputs/calculations **/
    private int MAXUPH, AVGUPH, seq;
    private double duration, MAX_CT_PASS, MIN_CT_PASS, AVG_CT_PASS, AVG_CT_FAIL, MAX_CT_FAIL, MIN_CT_FAIL;
    private double SDV_CT_PASS, SDV_CT_FAIL;  // standard deviations for CT
    private Set<String> process_issues = new HashSet<String>();
    private String[] pi_array = {"AmIOk","Battery Capacity","Battery Present","Battery Pairing test",
                "Burn Battery SN","Burn CG SN","Burn DCLR","Burn Front Camera FCMS","Burn Regn",
                "Burn LCM SN","Burn MdlC","Burn MPN","Burn NC Camera BCMB","Burn NC Camera BCMS",
                "Burn SN","Burn Syscfg OPTS","Check Sealed Mode","Clear Test No Light","Check UOP",
                "Clear Test With Light","Compare NandSize Range With SFC","Compare RSKU With SFC",
                "Find Front Camera","Find NC Camera","Get Data From SFIS","LED STROBE FLASH MODE",
                "Network CB","Pmuadc_BIST amuxa2","Pmuadc_BIST ildo11","Pmuadc_tbat","Read MLB SN",
                "Scan ISN","Speaker SN","TEMP_CPU","TEMP_RFPA","TEMP_TOP","TOUCH CSIG","Upload CG 70 SN",
                "FATAL ERROR UNIT_OUT_OF_PROCESS [ACTIVE]: CG 70ISN+ISN LCD NO PASS RECORD","FATAL ERROR",
                "Button Test"};
 
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    
    /** Process issues Hashset **/
    //for (int pi=0; pi<pi_array.length; pi++){
    //  process_issues.add(pi_array[pi]);
    //} 
       
    /** Critical output counts **/
    int inputCount = 0;
    int fail = 0;
    int failProcessIssues = 0;
    double durPTmp = 0, durFTmp = 0;
    int retestCnt = 0, retestAbnormal = 0, retestOverHalfHour = 0;
    /** The below hashmap is for consolidating the results of a device under the same grpnm
        into its last appeared result during the series **/
    HashMap<String, String> consolidate = new HashMap<String, String>();
    HashMap<String, String> consolidate1 = new HashMap<String, String>();
    /** Initiation of parameters **/
    duration = 0;
    calUPH uph = new calUPH(); // objects for calculating UPH
    statistics_func stats_func = new statistics_func();
    timeStamp = "";testStr = "";
    inputKey = key.toString();
    MAX_CT_PASS = 0;MAX_CT_FAIL = 0;MIN_CT_PASS = 0;MIN_CT_FAIL = 0;
    
    /** Receiving values from the mapper **/
    while (values.hasNext()){
      /** Input strings manipulation **/
      Text value = (Text) values.next();
      String seqTimeRes[] = value.toString().split("#"); // [0]=seq [1]=result:time:sn:duration
      seq = Integer.parseInt(seqTimeRes[0]); // sequence number in int form
      // [0]=result, [1]=time,[2]=sn,[3]=duration,[4]=failSymptoms
      String timeRes[] = seqTimeRes[1].toString().split("}"); 
      failSymp = timeRes[3];
      result = timeRes[0];
      time = timeRes[1];
      sn = timeRes[2];
      duration = Double.parseDouble(timeRes[3]);

      /** parameters to compare who comes as the last sn **/
      String pastTime = new String();
      String pastSeqs = new String();
      double currentSeq = Double.parseDouble(time+seq);
      
      /** Hash the results for manipulation in the next section of function **/      
      if(!consolidate.containsKey(sn)){
        if (result.equals("P"))
          stats_func.addDurationPass(sn, duration); // add to the hashset for stdev_CT calculations
        else
          stats_func.addDurationFail(sn, duration); // add to the hashset for stdev_CT calculations
        consolidate.put(sn, result+";"+time+","+seq+";"+duration+";"+failSymp);
      }
      else if (consolidate.containsKey(sn)){
        if (result.equals("P"))
          stats_func.addDurationPass(sn, duration); // add to the hashset for stdev_CT calculations
        else
          stats_func.addDurationFail(sn, duration); // add to the hashset for stdev_CT calculations
        String prevStr = new String();
        prevStr = consolidate.get(sn);
        prevStr = prevStr + "#" + (result+";"+time+","+seq+";"+duration+";"+failSymp);  
        consolidate.put(sn, prevStr);    
      }
      uph.addUPH(sn, time, result); // Insert the required information for recording UPH        
    }   
    
    /** Consolidating the results as the final one occured within the same grpnm/timestamp
        Also calculate the retest related issues (Abnormal when retest count >= 4)     **/
    Iterator itr = consolidate.entrySet().iterator();
    while (itr.hasNext()) {
        Map.Entry pairs = (Map.Entry)itr.next();
        String finalResult = new String();
        String initResult = new String();
        String timeOut = new String();
        String seqOut = new String();
        String durationOut = new String();
        String fail_symptoms = new String();
        double currentMin = 99999999999999.0;                            
        double currentMax = 0.0;
        // Start the consolidation
        String allInfo[] = pairs.getValue().toString().split("#");
        double[] allTimes = new double[allInfo.length];
        for (int m=0; m<allInfo.length; m++){
          String infoSplit[] = allInfo[m].split(";");
          String time_seq[] = infoSplit[1].split(",");
          double timings = Double.parseDouble(time_seq[0]+time_seq[1]);
          allTimes[m] = Double.parseDouble(time_seq[0]);
          if(timings > currentMax){ // Consolidate the results as the final one
            currentMax = timings;
            finalResult = infoSplit[0];
            timeOut = time_seq[0];
            seqOut = time_seq[1];
            durationOut = infoSplit[2];
            fail_symptoms = infoSplit[3];
          }
          if(timings <= currentMin){
            currentMin = timings;
            initResult = infoSplit[0];
          }
        }        
        if(finalResult.equals("F")){
          fail++;
          /** Process Issues checking **/
          for(int pi=0; pi<pi_array.length; pi++){
            if(fail_symptoms.matches("(.*)"+pi_array[pi]+"(.*)")){
              failProcessIssues++;
              break;
            }
          }
          //
        }
        inputCount++;
        if(finalResult.equals("P") && initResult.equals("F")){
          retestCnt++;
          if(allInfo.length >= 4)
            retestAbnormal++;
        }
        int judgeOverHalfHour = 0;
        double gap, gapNext = 9999999999.0;
        for(int j=0; j<allTimes.length; j++){
          gap = allTimes[j] - currentMin;
          if (gap > 0 && gap < gapNext){
            gapNext = gap;
          }
        }
        if(gapNext > 3000.0 && gapNext < 9999999999.0){
          retestOverHalfHour++;
        }
        itr.remove(); // avoids a ConcurrentModificationException
        consolidate1.put(pairs.getKey().toString(), finalResult+";"+timeOut+","+seqOut+";"+durationOut);
    }    
    
    /** Print out the input SN for debugging **/
    if(inputKey.equals("3F01,SA-FACT2")){
      // sorted order with respect to sn
      testStr = "\n";
      String toTest = new String();
      SortedSet<String> SNs = new TreeSet<String>(consolidate1.keySet());
      for (String snIn : SNs) { 
        String SNINFO = consolidate1.get(snIn);
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
        
    /** Aggregate and format all information needed for the output **/
    DecimalFormat df2 = new DecimalFormat("###.##");
    timeStamp += "LAST";
    inputFail = inputCount + "," + fail;
    double inputCntNum = new Double(inputCount);
    double failCntNum = new Double(fail);
    double yield = (inputCntNum-failCntNum) / inputCntNum * 100.00;
    double YIELD = new Double(df2.format(yield)).doubleValue();
    double retestRatioTmp = ((double) retestCnt) / inputCntNum * 100.00;
    double retestRatio = new Double(df2.format(retestRatioTmp)).doubleValue();   
    double retestAbnormalTmp = ((double) retestAbnormal) / inputCntNum * 100.00;
    double retestAbnormalRatio = new Double(df2.format(retestAbnormalTmp)).doubleValue();    
    int trueFail = fail - failProcessIssues; 
    double retestOverHalfHourTmp = ((double) retestOverHalfHour) / inputCntNum * 100.00;
    double retestOverHalfHourRatio = new Double(df2.format(retestOverHalfHourTmp)).doubleValue();
    SDV_CT_PASS = new Double(df2.format(SDV_CT_PASS)).doubleValue();
    SDV_CT_FAIL = new Double(df2.format(SDV_CT_FAIL)).doubleValue();
    AVG_CT_PASS = new Double(df2.format(AVG_CT_PASS)).doubleValue();
    AVG_CT_FAIL = new Double(df2.format(AVG_CT_FAIL)).doubleValue();
    /** Prinint the outputs **/    
    outValue = inputFail + ",YIELD=" + YIELD + ",Retest=" + retestCnt + ",RetestRatio=" + retestRatio
               + ",RetestAbnormal=" + retestAbnormal + ",RetestAbnormalRatio=" + retestAbnormalRatio 
               + ",TrueFail=" + trueFail + ",Proccess_Issue=" + failProcessIssues
               + ",RetestOverHalfHour=" + retestOverHalfHour + ",RetestOverHalfHourRatio=" + retestOverHalfHourRatio
               + ",AVGUPH=" + AVGUPH + ",MAXUPH=" + MAXUPH 
               + ",StdDev_CT_Pass=" + SDV_CT_PASS + ",AVG_CT_PASS=" + AVG_CT_PASS 
               + ",MIN_CT_PASS=" + MIN_CT_PASS + ",MAX_CT_PASS=" + MAX_CT_PASS
               + ",StdDev_CT_FAIL=" + SDV_CT_FAIL + ",AVG_CT_FAIL=" + AVG_CT_FAIL 
               + ",MIN_CT_FAIL=" + MIN_CT_FAIL + ",MAX_CT_FAIL=" + MAX_CT_FAIL
               + "\nTime,SN,Detail" + testStr + "\nEND\n";
               //+ " UPHLog: " + uph.getUPHLog();

    outVal.set(outValue);
    outKey.set(inputKey);
    output.collect(outKey, outVal);
	}
}
