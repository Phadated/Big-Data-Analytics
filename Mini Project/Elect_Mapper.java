import java.io.IOException;
import java.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;

public class Elect_Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private IntWritable key=new IntWritable();


    int minmon=0;
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {
       
        super.setup(context);
        BufferedReader br=new BufferedReader( new InputStreamReader(new FileInputStream("/home/kjsce/Desktop/Elect/elect.txt")));
        String s1;
        int i=0,c=0;
        int year=0;
        int jan=0;
        int feb=0;
        int mar=0,apr=0,may=0,jun=0,jul=0,aug=0,sep=0,oct=0,nov=0,dec=0;
        HashMap<String,String> hm=new HashMap();
        while((s1=br.readLine())!=null)
        {
        	//System.out.println(s1);
        String s2[]=s1.split("   ");
      /* jan+=Integer.parseInt(s2[1]);
       feb+=Integer.parseInt(s2[2]);
       mar+=Integer.parseInt(s2[3]);
       apr+=Integer.parseInt(s2[4]);
       may+=Integer.parseInt(s2[5]);
       jun+=Integer.parseInt(s2[6]);
       jul+=Integer.parseInt(s2[7]);
       aug+=Integer.parseInt(s2[8]);
       sep+=Integer.parseInt(s2[9]);
       oct+=Integer.parseInt(s2[10]);
       nov+=Integer.parseInt(s2[11]);
       dec+=Integer.parseInt(s2[12]);*/
        
       //System.out.println(s2[1]);
       //hm.put(s2[0],s2[1]+s2[2]+s2[3]+s2[4]+s2[5]+s2[6]+s2[7]+s2[8]+s2[9]+s2[10]+s2[11]+s2[12]);
       c++;
        }
        jan/=c;
        feb/=c;
        mar/=c;
        apr/=c;
        may/=c;
        jun/=c;
        jul/=c;
        aug/=c;
        sep/=c;
        oct/=c;
        nov/=c;
        dec/=c;
       
    }
    public void map(LongWritable ikey, Text ivalue, Context context)
            throws IOException, InterruptedException {
        String line=ivalue.toString();
        String[] tokens=StringUtils.split(line,' ');
        key.set(Integer.parseInt(tokens[0]));
        Text t = new Text(Integer.parseInt(tokens[1])+" "+Integer.parseInt(tokens[2])
        		+" "+Integer.parseInt(tokens[3])+" "+Integer.parseInt(tokens[4])
        		+" "+Integer.parseInt(tokens[5])+" "+Integer.parseInt(tokens[6])
        		+" "+Integer.parseInt(tokens[7])+" "+Integer.parseInt(tokens[8])
        		+" "+Integer.parseInt(tokens[9])+" "+Integer.parseInt(tokens[10])
        		+" "+Integer.parseInt(tokens[11])+" "+Integer.parseInt(tokens[12]));
        //System.out.println(t);
        context.write(key,t);
    }

}