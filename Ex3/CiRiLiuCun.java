package HadoopProject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

//1. map任务处理
//1.1 读取输入文件内容，解析成key、 value对。 对输入文件的每一行，解析成key、value对。每一个键值对调用一次map函数。
//1.2 写自己的逻辑，对输入的key、 value进行处理，转换成新的key、 value输出。
//1.3 对输出的key、 value进行分区。
//1.4 对不同分区的数据，按照key**进行排序、分组**。相同key的value放到一个集合中。
//1.5 (可选)分组后的数据进行归约。
//
//2.reduce任务处理
//2.1 对多个map任务的输出，按照不同的分区，通过网络copy到不同的reduce节点。
//2.2 对多个map任务的输出进行合并、排序。 写reduce函数自己的逻辑，对输入的key、 values处理，转换成新的key、 value输出。
//2.3 把reduce的输出保存到文件中。
public class CiRiLiuCun {
	/*
	 * Map类继承自MapReduceBase，并且它实现了Mapper接口，此接口是一个规范类型，它有4种形式的参数，
	 * 分别用来指定map的输入key值类型、输入value值类型、输出key值类型和输出value值类型。在本例中，
	 * 因为使用的是TextInputFormat，它的输出key值是LongWritable类型，输出value值是Text类型，
	 * 所以map的输入类型为<LongWritable,Text>。在本例中需要输出<word,1>这样的形式，因此输出的key值类型是Text，
	 * 输出的value值类型是IntWritable。
	 */
	public static class map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
		/*
		 * 　实现此接口类还需要实现map方法，map方法会具体负责对输入进行操作，
		 * 在本例中，map方法对输入的行以空格为单位进行切分，然后使用OutputCollect收集输出的<word,1>。
		 */
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, 
				Reporter reporter)throws IOException {
			String line=value.toString();
			String[] ip=line.split("\t");
			//\t分割 将key value都设置为ip 
			output.collect(new Text(ip[1]),one);
			
			
		}
	}
	/*
	 * 　Reduce类也是继承自MapReduceBase的，需要实现Reducer接口。Reduce类以map的输出作为输入，
	 * 因此Reduce的输入类型是<Text，Intwritable>。而Reduce的输出是单词和它的数目，
	 * 因此，它的输出类型是<Text,IntWritable>。Reduce类也要实现reduce方法，在此方法中，
	 * reduce函数将输入的key值作为输出的key值，然后将获得多个value值加起来，作为输出的值。
	 */
	public static class reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while(values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
			

			//创建一个HashSet对象 并定义泛型类型
//			Set<Text> set=new HashSet<Text>();
//			while(values.hasNext()){
//				//一个个传入元素
//				Text t=new Text(values.next());
//				//添加一个元素
//				set.add(t);
//			}
//			//依次取出集合中元素
//			for(Text s:set){
//				output.collect(key,new Text(""+set.size()));
//			}
	
		}
	}
	
	public static void main(String[] args) throws IOException {
		//调用Jobconf类来对MapReduce Job进行初始化
		JobConf conf = new JobConf(CiRiLiuCun.class);
		conf.setJobName("CRLC");
		/*
		 * 接着设置Job输出结果<key,value>的中key和value数据类型，
		 * 因为结果是<单词,个数>，所以key设置为"Text"类型，相当于Java中String类型。
		 * Value设置为"IntWritable"，相当于Java中的int类型。
		 */
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		/*
		 * 然后设置Job处理的Map（拆分）、Combiner（中间结果合并）以及Reduce（合并）的相关处理类。
		 * 这里用Reduce类来进行Map产生的中间结果合并，避免给网络数据传输产生压力。
		 */
		conf.setMapperClass(map.class);
		conf.setNumReduceTasks(2);
		//conf.setCombinerClass(reduce.class);
		conf.setReducerClass(reduce.class);
		
		//接着就是调用setInputPath()和setOutputPath()设置输入输出路径。
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//任务数设为2
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
