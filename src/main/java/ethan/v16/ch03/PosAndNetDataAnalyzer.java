package ethan.v16.ch03;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tools.ant.taskdefs.PathConvert.MapEntry;

import ethan.v16.ch03.PosAndNetObject.AnalysisException;

/**
 * @since 2014-9-27
 * @author ethan
 */

public class PosAndNetDataAnalyzer extends Configured implements Tool {

	// 0000000000|00-09 00000186|1378931007

	// setup
	// map
	public static Date logDate;

	public PosAndNetDataAnalyzer() {
	}

	// java.lang.RuntimeException: java.lang.NoSuchMethodException:
	// ethan.v16.ch03.PosAndNetDataAnalyzer$PosAndNetMapper.<init>()
	// ^^mapper和reduce都必须是public static 类型的
	// ^^ 一代map方法把文件的行号当成key,所以要用LongWritable。
	public static class PosAndNetMapper extends Mapper<LongWritable, Text, Text, Text> {
		String asdate, ashours;
		boolean analysisPos;
		PosAndNetObject object = new PosAndNetObject();

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {

			try {
				object.set(value.toString(), analysisPos, ashours, asdate);
				PosAndNetDataAnalyzer.logDate = object.logDate;
				context.write(object.keyOut(), object.valueOut());
			} catch (AnalysisException e) {
				context.getCounter(e.getCounter()).increment(1);
				e.printStackTrace();
			}
		};

		protected void setup(Context context) throws java.io.IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			asdate = conf.get("analysisDate");
			ashours = conf.get("analysisHours");
			// ^^ 怎样获得文件名
			// context 获得，不是conf获得
			FileSplit fs = (FileSplit) context.getInputSplit();
			String fileName = fs.getPath().getName();
			if (fileName.startsWith("POS"))
				analysisPos = true;
			else if (fileName.startsWith("NET"))
				analysisPos = false;
			else
				throw new IOException("file not named start with \"POS\" or \"NET\" ");

		};

	}
	
	public static class PosAndNetReducer extends Reducer<Text, Text, NullWritable, Text> {
		private String date;
		private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		protected void setup(Context context) throws IOException, InterruptedException {
			this.date = context.getConfiguration().get("analysisDate");
		};

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String imsi = key.toString().split("\\|")[0];
			String flag = key.toString().split("\\|")[1];
			// 时间排序
			Iterator<Text> iterator = values.iterator();
			TreeMap<Long, String> times = new TreeMap<Long, String>(); // 对key升序
			while (iterator.hasNext()) {
				Text text = iterator.next();
				String pos = text.toString().split("\\|")[0];
				String t = text.toString().split("\\|")[1];
				times.put(Long.valueOf(t), pos);
			}
			// 加上最后一个元素（flag[1]-00:00,OFF）;
			try {
				Date date = formatter.parse(this.date + " " + flag.split("-")[1] + ":00:00");
				times.put(date.getTime(), "OFF");
			} catch (ParseException e) {
				e.printStackTrace();
			}

			HashMap<String, Float> countedTimes = countStayTime(times);

			for (Map.Entry<String, Float> entry : countedTimes.entrySet()) {
				StringBuffer sb = new StringBuffer();
				sb.append(imsi).append("|").append(entry.getKey()).append("|").append(flag).append("|").append(entry.getValue());
				context.write(NullWritable.get(), new Text(sb.toString()));
			}
		}
	}
	
	/**
	 * @param times
	 * @return
	 */
	private static HashMap<String, Float> countStayTime(TreeMap<Long, String> times) {

		HashMap<String, Float> results = new HashMap<String, Float>();
		Iterator<Entry<Long, String>> it = times.entrySet().iterator();
		Entry<Long, String> preOne, nextOne;
		preOne = it.next();
		while (it.hasNext()) {
			nextOne = it.next();
			// 相邻时间相减
			float stay = (nextOne.getKey() - preOne.getKey()) / 60.0f; // 分钟
			// 时间间隔过大则代表关机
			if (stay > 60.0)
				continue;
			String key = preOne.getValue();
			if (results.containsKey(key)) {
				stay += results.get(key);
			}
			results.put(key, stay);
			preOne = nextOne;  

		}
		return results;
	}
	

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		// ^^ 一代怎么设mapper
		// 用job不是用conf设置
		conf.set("analysisDate", args[2]);
		conf.set("analysisHours", args[3]);
		// 1.名字 (4个总是忘记)
		Job job = new Job(conf, "ETHAN 'S MAPREDUCE JOB");
		// 2.旧版需要jar
		job.setJarByClass(PosAndNetDataAnalyzer.class);
		// 3.输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 4.输出样式
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(PosAndNetMapper.class);
		job.setReducerClass(PosAndNetReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setOutputFormatClass()
		job.waitForCompletion(true);

		return job.isSuccessful() ? 0 : 1;
	}

	// ^^不处理异常就抛给hadoop框架
	public static void main(String[] args) throws Exception {
		String[] args2 = new String[4];
		args2[0] = "hdfs://u1:9000/home/hadoop/data.v16/POS.ch03";
		String outputPath = args2[1] = "hdfs://u1:9000/home/hadoop/out.v16";
		args2[2] = "2013-09-12";
		args2[3] = "09-17-24";
		// 输入地址 输出地址 统计日期 统计区间
		if (args2.length != 4) {
			System.err.println("wrong input parameter!");
			System.err.println("correct parameter as :");
			System.err.println("eg:<inputpath> <outputpath> <analysisDate> <analysisHours>");

			System.exit(-1);
		}
		Configuration conf = new Configuration();
		CommonUtil.deleteOutput(outputPath, conf);
		int res = ToolRunner.run(conf, new PosAndNetDataAnalyzer(), args2);
		System.exit(res);
	}

}
