package edu.ucr.cs.cs226.sbala027;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.*;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class KNN 
{

    public static class TokenizerMapper extends Mapper<Object, Text, DoubleWritable, Text>
    {

        private String qptx;
	private String qpty;
	private Double point1, point2;
	private Text p = new Text();
	private DoubleWritable Distance = new DoubleWritable();
	private String[] points;
        
        @Override
        public void setup(Context con) throws IOException, InterruptedException 
	    {
            Configuration c=con.getConfiguration();
            qptx = c.get("QueryPointX");
            qpty = c.get("QueryPointY");
            
	        point1 = Double.parseDouble(qptx);
            point2 = Double.parseDouble(qpty);
	    }
        public void map(Object key, Text t, Context con) throws IOException, InterruptedException 
	    {
            
            Double dist;
            String str = t.toString();

            /* Split the string */
            String[] aftersplit = str.split(",");

            /* Take the 2nd and 3rd values as points, since the 1st value is ID */
            double dpt1 = Double.parseDouble(aftersplit[1]);
            double dpt2 = Double.parseDouble(aftersplit[2]);

            /* Calculates the Euclidean distance */
            dist = (Math.sqrt(Math.pow((point1-dpt1),2) + Math.pow((point2-dpt2),2)));

            /* Output(distance, point) as key value pair */
            Distance.set(dist);
            p.set(str);
            con.write(Distance, p);
        }
    }

    public static class IntSumReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> 
    {
        private int count_val = 1;

        protected void setup(Context con) 
	    {
            count_val = 1;
        }

        public void reduce(DoubleWritable d, Iterable<Text> t, Context con) throws IOException, InterruptedException 
	    {
            Configuration c = con.getConfiguration();
            int x = c.getInt("k", 1);
            if (count_val <= x) 
	        {
                for (Text p : t) 
		        {
                    con.write(d, p);
                    count_val++;
                }
            }
        }
    }
        /* Arguments passed
         args[0] --> Input path name
         args[1] --> Input query point X
	     args[2] --> Input query point Y
         args[3] --> k (Top k distances)
         args[4] --> Output path name
        */
    public static void main( String[] args )throws IOException
    {
        Configuration c = new Configuration();
        c.set("QueryPointX",args[1]);
	    c.set("QueryPointY",args[2]);
        c.setInt("k", Integer.parseInt(args[3]));
        Job job = Job.getInstance(c, "KNN");
        job.setJarByClass(KNN.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        try 
	    {
            job.waitForCompletion(true);
        }
	    catch (InterruptedException e)
        {
            e.printStackTrace();
        } 
	    catch (ClassNotFoundException e)
	    {
            e.printStackTrace();
        }
    }

}