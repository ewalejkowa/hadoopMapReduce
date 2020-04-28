package popularity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.io.Writable;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
        public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Links Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(PopularityLeague.LinkCountMap.class);
        jobA.setReducerClass(PopularityLeague.LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Links");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(PopularityLeague.IntArrayWritable.class);

        jobB.setMapperClass(PopularityLeague.TopLinksMap.class);
        jobB.setReducerClass(PopularityLeague.TopLinksReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split(" ");
            splits[0]=splits[0].replace(":","");
            Integer val = Integer.parseInt(splits[0]);
            context.write(new IntWritable(val), new IntWritable(0));
            for (int i=1;i<splits.length;i++){
                Integer l = Integer.parseInt(splits[i]);
                context.write(new IntWritable(l),new IntWritable(1));
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
            @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
               context.write(  key,new IntWritable(sum) );
           
        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;
        List<String> league;
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");
            league = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
            this.N = 16;
        }
         TreeSet<Pair> list = new TreeSet<Pair>(new Comparator<Pair>() {
    
    public int compare(Pair one, Pair two){
        if ((int)one.second > (int)two.second) return 1;
        else if (one.second == two.second){
            return one.first.compareTo(two.first);
        }
        else return -1;
    }});
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Pair<Integer, Integer> y =  new Pair<Integer,Integer>(Integer.parseInt(key.toString()),Integer.parseInt(value.toString()));          
            list.add(y);
        }
        public boolean inLeague(String val){
            for (String l :league){
                if (l.equals(val))return true;
            }
            return false;
        }
        @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {  
             NullWritable t = NullWritable.get();
                   Integer[] result = new Integer[2*league.size()];
                   int i=0;
                   int y=0;
                    for (Pair e:list)
                    {
                          String g = e.toString();
                         g = g.replace("(", "");
                              g = g.replace(")", "");
                              g = g.replace(" ", "");
                              String[] r=g.toString().split(",");
                               
                             if (inLeague(r[0])){
                                result[y] = Integer.parseInt(r[0]);
                                y++;
                                result[y] = Integer.parseInt(r[1]);                         
                                y++;
                             }
                     i++;                 
                    }
                    for (String l :league){
                        System.out.println(l);
                    }
                    IntArrayWritable result2 = new IntArrayWritable(result);
                     context.write(t,result2);
      }        

    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;
        List<String> league;
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");
            league = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
            this.N = 16;
        }

        public int getLeague(String val){
            for(int i=0; i<league.size();i++){                
                if (val.equals(league.get(i)))return i;
            }
            return 0;
        }
        public void reduce(NullWritable n, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
          int y=0;
          IntWritable tmp =  new IntWritable(0);
                   HashSet<Integer> league_number = new HashSet<Integer>();
          List pages_list =  new ArrayList();
          List value_list = new ArrayList();
                   Integer number=0;
            for (IntArrayWritable g:values)
			{
            y++;
             for(Writable w: g.get()) {
                  String tmp2 = w.toString();
                  Integer val = Integer.parseInt(tmp2);
                   if (y%2==0){
                       league_number.add(Integer.parseInt(tmp2.toString()));
                       value_list.add(tmp2);
                       pages_list.add(tmp);
                   }
                   else{                      
                       tmp=new IntWritable(Integer.parseInt(tmp2));
                   }                  
                   y++;      
             }
          }
            y=1;
            number = league_number.size();
            int o=0;
            String previous="";
       Integer t=0;
       for(int i=0; i<pages_list.size() && o<=number;i++)
        {
          Object tmp2 = pages_list.get(i);           
          context.write(new IntWritable(Integer.parseInt(tmp2.toString())),new IntWritable(o));
          String tmp_previous="";
           if (i<(pages_list.size()-1)){
              tmp_previous=value_list.get(i+1).toString();          
           }
           if (!tmp_previous.equals(previous) && !previous.equals("")) {
               o=o+1+t;
               t=0;
           }
           else {
               t++;
           }
            previous =tmp_previous;
          }           
        }      
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}