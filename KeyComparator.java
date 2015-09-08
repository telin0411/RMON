package rmon;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.lang.Math;
import org.apache.hadoop.io.*;

public class KeyComparator extends WritableComparator {

       protected KeyComparator(){
                 super(Text.class, true);
       }
       @Override
       public int compare(WritableComparable w1, WritableComparable w2){
              Text t1 = (Text) w1;
              Text t2 = (Text) w2;
              String str1 = new String();
              String str2 = new String();
              str1 = t1.toString();
              str2 = t2.toString();
              //double val1 = Double.parseDouble(str1);
              //double val2 = Double.parseDouble(str2);              
              int result = str1.compareTo(str2);
              return result;
       }

}
