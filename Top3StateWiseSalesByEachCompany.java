package com.bigdata.acadgild;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Top3StateWiseSalesByEachCompany {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration con = new Configuration();
		Job job = new Job(con);
		job.setJarByClass(Top3StateWiseSalesByEachCompany.class);
		
		job.setMapOutputKeyClass(Company.class);
		job.setMapOutputValueClass(StateCount.class);
		
		job.setMapperClass(TotalUnitsSoldByCompanyMapper.class);
		job.setCombinerKeyGroupingComparatorClass(CombinerKeyComparator.class);
		job.setCombinerClass(TotalUnitsSoldByCompanyCombiner.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		job.setSortComparatorClass(ReducerKeyComparator.class);
		job.setGroupingComparatorClass(ReducerKeyComparator.class);
		job.setReducerClass(TotalUnitsSoldByCompanyReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
}
	
	public static class TotalUnitsSoldByCompanyMapper extends Mapper<LongWritable, Text, Company, StateCount>
	{
		private static final String NA = "NA";
		private Company company = new Company();
		private StateCount sc = new StateCount();
		
		public void map(LongWritable key, Text value, Context context ) throws IOException,InterruptedException
		{
			String[] values = value.toString().split("\\|");
			if ( !NA.equals(values[0]) &&  !NA.equals(values[1]))  {
				company.setName(new Text(values[0]));
				company.setState(new Text(values[3]));
				sc.setState(new Text(values[3]));
				sc.setCount(new IntWritable(1));
				context.write(company, sc);
			}
		}
	}
	
	// combiner implementation
	public static class TotalUnitsSoldByCompanyCombiner extends Reducer<Company, StateCount, Company, StateCount>
	{
		private Integer minValue = Integer.MIN_VALUE;
		private IntWritable total = new IntWritable();
		private StateCount sc = new StateCount();
		
		@Override
		protected void setup(Reducer<Company, StateCount, Company, StateCount>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}
		
		public void reduce(Company key, Iterable<StateCount> values,Context context) throws IOException, InterruptedException {
			Integer totalUnits = 0;
			for ( StateCount value : values ) 
			{
				sc.setState(value.getState());
				if ( value.count.get() > minValue )
				{
					totalUnits+=value.count.get();
				}
			}
			total.set(totalUnits);
			sc.setCount(total);
			context.write(key, sc);
		}
		
		@Override
		protected void cleanup(Reducer<Company, StateCount, Company, StateCount>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
	// reducer logic
	public static class TotalUnitsSoldByCompanyReducer extends Reducer<Company, StateCount, Text, Text>
	{
		private Integer c;
		private HashMap<String, Integer> map = new HashMap<>();
		private SortedSet<StateCount> ss = new TreeSet<>();
		
		@Override
		protected void setup(Reducer<Company, StateCount, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}
		
		public void reduce(Company key, Iterable<StateCount> values,Context context) throws IOException, InterruptedException {
			for ( StateCount value : values ) 
			{
				if ( map.containsKey(value.state.toString()))
				{
					c = map.get(value.state.toString());
					c += value.count.get();
					map.put(value.state.toString(),c);
				}
				else
				{
					map.put(value.state.toString(), value.count.get());
				}
			}
			
			
			for ( String state : map.keySet() )
			{
				StateCount sc = new StateCount();
				sc.setState(new Text(state));
				sc.setCount(new IntWritable(map.get(state)));
				ss.add(sc);
			}
			
			int i = 0;
			for ( StateCount stateCount : ss )
			{
				i++;
				if ( i <= 3 )
					context.write(key.name, new Text(stateCount.state.toString().concat(" ").concat(String.valueOf(stateCount.count.get()))));				
			}
			/*for ( String k : map.keySet() )
			{
				context.write(key.name, new Text(k.concat(" ").concat(String.valueOf(map.get(k)))));
			}*/
			
			map.clear();
			ss.clear();
		}
		
		@Override
		protected void cleanup(Reducer<Company, StateCount, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
	
	private static class Company implements WritableComparable<Company>
	{
		private Text name;
		private Text state;
		
		public Company() { name = new Text(); state = new Text(); }

		public void setName(Text name) {
			this.name = name;
		}
		
		public Text getName() {
			return name;
		}

		public void setState(Text state) {
			this.state = state;
		}
		
		public Text getState() {
			return state;
		}

		public int compareTo( Company other ) 
		{
			int c = name.compareTo(other.name);
			if ( c == 0 )
				return state.compareTo(other.state);
			return c;
		}

		@Override
		public String toString() {
			return  name.toString().concat(" ").concat(state.toString());
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			name.readFields(in);
			state.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			name.write(out);
			state.write(out);
		}
	}
	
	private static class StateCount implements WritableComparable<StateCount>
	{
		private Text state;
		private IntWritable count;

		public StateCount() { state = new Text(); count = new IntWritable(0); } 
		
		public void setCount(IntWritable count) {
			this.count = count;
		}
		
		public void setState(Text state) {
			this.state = state;
		}
		
		public IntWritable getCount() {
			return count;
		}
		
		public Text getState() {
			return state;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			state.readFields(in);
			count.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			state.write(out);
			count.write(out);
		}
		
		@Override
		public String toString() {
			return state.toString().concat(" ").concat(count.toString());
		}
		
		@Override
		public boolean equals(Object obj) {
			StateCount o = (StateCount) obj;
			boolean e = state.equals(o.state);
			return e;
		}
		
		@Override
		public int hashCode() {
			return state.hashCode();
		}
		
		@Override
		public int compareTo(StateCount o) {
			int c = count.compareTo(o.getCount());
			if ( c == 0 )
				return state.compareTo(o.getState());
			return -1 * c;
		}
	}

	// Sorting records based on name for reducer
	private static class ReducerKeyComparator extends WritableComparator
	{
		protected ReducerKeyComparator() {
			super(Company.class,true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Company ca = (Company) a;
			Company cb = (Company) b;
			return ca.getName().compareTo(cb.getName());
		}
	}
	
	// grouping records based on key for combiner
	private static class CombinerKeyComparator extends WritableComparator
	{
		protected CombinerKeyComparator() {
			super(Company.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Company ca = (Company) a;
			Company cb = (Company) b;
			return ca.compareTo(cb);
		}
	}	

}