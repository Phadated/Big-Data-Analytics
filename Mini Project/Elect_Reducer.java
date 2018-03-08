import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Elect_Reducer extends Reducer<IntWritable,Text,String,String> {

	 static int maxelect=1000;
	 static int max=0,min=500000;
	  static int maxyear,minyear,mi=0,ma=0;
	 static float avg=0;
	 static int jan=0;
     static int feb=0;
     static int mar=0,apr=0,may=0,jun=0,jul=0,aug=0,sep=0,oct=0,nov=0,dec=0;
     int a=0;
    public void reduce(IntWritable _key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // process values
       
        Iterator<Text> iterator=values.iterator();
        Text b=new Text(iterator.next());
        String br=""+b;
        String s[]=br.split(" ");
        
        
        //System.out.println(s[1]);
        int c=0;
        //while(c++!=1000)
       // {
        	jan+=Integer.parseInt(s[0]);
        	feb+=Integer.parseInt(s[1]);
        	mar+=Integer.parseInt(s[2]);
        	apr+=Integer.parseInt(s[3]);
        	may+=Integer.parseInt(s[4]);
        	jun+=Integer.parseInt(s[5]);
        	jul+=Integer.parseInt(s[6]);
        	aug+=Integer.parseInt(s[7]);
        	sep+=Integer.parseInt(s[8]);
        	oct+=Integer.parseInt(s[9]);
        	nov+=Integer.parseInt(s[10]);
        	dec+=Integer.parseInt(s[11]);
        	a=Integer.parseInt(s[0])+Integer.parseInt(s[1])+Integer.parseInt(s[2])
        			+Integer.parseInt(s[3])+Integer.parseInt(s[4])+Integer.parseInt(s[5])
        			+Integer.parseInt(s[6])+Integer.parseInt(s[7])+Integer.parseInt(s[8])
        			+Integer.parseInt(s[9])+Integer.parseInt(s[10])+Integer.parseInt(s[11]);
        	
       // }
        //System.out.println(jan+" "+feb+" "+mar+" "+apr+" "+may+" "+jun+" "+jul+" "+aug+" "+sep+" "+oct+" "+nov+" "+dec);
       
        avg+=a;
        
        if(a>max)
        	{max=a;
        	maxyear=_key.get();
        	//System.out.println(maxyear);
        	}
        if(a<min)
    	{min=a;
    	minyear=_key.get();
    	//System.out.println(maxyear);
    	}
        maxelect--;
        
       
       
        if(maxelect==0)
        {

        	 IntWritable maxx=new IntWritable(max);
             IntWritable minn=new IntWritable(min);
        	
        	int arr[]={jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec};
        	//System.out.println(arr[0]);
        	String min=findmin(arr);
        	String max=findmax(arr);
        	String mini[]=min.split(" ");
        	String maxi[]=max.split(" ");
        //System.out.println("t1"+maxyear);
        avg=avg/1000;
        //context.write(new IntWritable(),new IntWritable(avg));
        context.write("Year : "+new IntWritable(maxyear)+" has the maximum annual consumption of "+maxx
        +"\nYear : "+new IntWritable(minyear)+" has the minimum annual consumption of "+minn,
        		
        		"\nThe month of "+mini[1]+" has the minimum total consumption of "+mini[0]+" (monthwise)"
        		+"\nThe month of "+maxi[1]+" has the maximum total consumption of "+maxi[0]+" (monthwise)"
        		+"\nThe average annual consumption is : "+avg+"\n"
        		+"The month of "+mini[1]+" has the minimum average consumption of "+Float.parseFloat(mini[0])/1000
        		+"\nThe month of "+maxi[1]+" has the maximum average consumption of "+Float.parseFloat(maxi[0])/1000);
        }
    }
    
    public static String findmin(int[] array){  
        int minValue = array[0]; 
        int index=0;
        String min="";
        for(int i=1;i<array.length;i++){  
        if(array[i] < minValue){  
        minValue = array[i];
        index=i;
           }
        
        }
        switch(index)
        {
        case 0:min="January";break;
        case 1:min="February";break;
        case 2:min="March";break;
        case 3:min="April";break;
        case 4:min="May";break;
        case 5:min="June";break;
        case 6:min="July";break;
        case 7:min="August";break;
        case 8:min="September";break;
        case 9:min="October";break;
        case 10:min="November";break;
        case 11:min="December";break;
        
        
        }

       return minValue+" "+min ;  
   }
    public static String findmax(int[] array){  
        int maxValue = array[0]; 
        int index=0;
        String max="";
        for(int i=1;i<array.length;i++){  
        if(array[i] > maxValue){  
        maxValue = array[i];
        index=i;
           }
        
        }
        switch(index)
        {
        case 0:max="January";break;
        case 1:max="February";break;
        case 2:max="March";break;
        case 3:max="April";break;
        case 4:max="May";break;
        case 5:max="June";break;
        case 6:max="July";break;
        case 7:max="August";break;
        case 8:max="September";break;
        case 9:max="October";break;
        case 10:max="November";break;
        case 11:max="December";break;
        
        
        }

       return maxValue+" "+max ;  
   }  
    

}