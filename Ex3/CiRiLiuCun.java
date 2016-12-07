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

//1. map������
//1.1 ��ȡ�����ļ����ݣ�������key�� value�ԡ� �������ļ���ÿһ�У�������key��value�ԡ�ÿһ����ֵ�Ե���һ��map������
//1.2 д�Լ����߼����������key�� value���д���ת�����µ�key�� value�����
//1.3 �������key�� value���з�����
//1.4 �Բ�ͬ���������ݣ�����key**�������򡢷���**����ͬkey��value�ŵ�һ�������С�
//1.5 (��ѡ)���������ݽ��й�Լ��
//
//2.reduce������
//2.1 �Զ��map�������������ղ�ͬ�ķ�����ͨ������copy����ͬ��reduce�ڵ㡣
//2.2 �Զ��map�����������кϲ������� дreduce�����Լ����߼����������key�� values����ת�����µ�key�� value�����
//2.3 ��reduce��������浽�ļ��С�
public class CiRiLiuCun {
	/*
	 * Map��̳���MapReduceBase��������ʵ����Mapper�ӿڣ��˽ӿ���һ���淶���ͣ�����4����ʽ�Ĳ�����
	 * �ֱ�����ָ��map������keyֵ���͡�����valueֵ���͡����keyֵ���ͺ����valueֵ���͡��ڱ����У�
	 * ��Ϊʹ�õ���TextInputFormat���������keyֵ��LongWritable���ͣ����valueֵ��Text���ͣ�
	 * ����map����������Ϊ<LongWritable,Text>���ڱ�������Ҫ���<word,1>��������ʽ����������keyֵ������Text��
	 * �����valueֵ������IntWritable��
	 */
	public static class map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
		/*
		 * ��ʵ�ִ˽ӿ��໹��Ҫʵ��map������map��������帺���������в�����
		 * �ڱ����У�map��������������Կո�Ϊ��λ�����з֣�Ȼ��ʹ��OutputCollect�ռ������<word,1>��
		 */
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, 
				Reporter reporter)throws IOException {
			String line=value.toString();
			String[] ip=line.split("\t");
			//\t�ָ� ��key value������Ϊip 
			output.collect(new Text(ip[1]),one);
			
			
		}
	}
	/*
	 * ��Reduce��Ҳ�Ǽ̳���MapReduceBase�ģ���Ҫʵ��Reducer�ӿڡ�Reduce����map�������Ϊ���룬
	 * ���Reduce������������<Text��Intwritable>����Reduce������ǵ��ʺ�������Ŀ��
	 * ��ˣ��������������<Text,IntWritable>��Reduce��ҲҪʵ��reduce�������ڴ˷����У�
	 * reduce�����������keyֵ��Ϊ�����keyֵ��Ȼ�󽫻�ö��valueֵ����������Ϊ�����ֵ��
	 */
	public static class reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while(values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
			

			//����һ��HashSet���� �����巺������
//			Set<Text> set=new HashSet<Text>();
//			while(values.hasNext()){
//				//һ��������Ԫ��
//				Text t=new Text(values.next());
//				//���һ��Ԫ��
//				set.add(t);
//			}
//			//����ȡ��������Ԫ��
//			for(Text s:set){
//				output.collect(key,new Text(""+set.size()));
//			}
	
		}
	}
	
	public static void main(String[] args) throws IOException {
		//����Jobconf������MapReduce Job���г�ʼ��
		JobConf conf = new JobConf(CiRiLiuCun.class);
		conf.setJobName("CRLC");
		/*
		 * ��������Job������<key,value>����key��value�������ͣ�
		 * ��Ϊ�����<����,����>������key����Ϊ"Text"���ͣ��൱��Java��String���͡�
		 * Value����Ϊ"IntWritable"���൱��Java�е�int���͡�
		 */
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		/*
		 * Ȼ������Job�����Map����֣���Combiner���м����ϲ����Լ�Reduce���ϲ�������ش����ࡣ
		 * ������Reduce��������Map�������м����ϲ���������������ݴ������ѹ����
		 */
		conf.setMapperClass(map.class);
		conf.setNumReduceTasks(2);
		//conf.setCombinerClass(reduce.class);
		conf.setReducerClass(reduce.class);
		
		//���ž��ǵ���setInputPath()��setOutputPath()�����������·����
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//��������Ϊ2
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
