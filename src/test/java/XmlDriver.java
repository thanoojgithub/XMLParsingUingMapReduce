package com;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class XmlDriver extends Configured implements Tool {

	public static class XmlInputFormat extends TextInputFormat {

		public static final String START_TAG_KEY = "xmlinput.start";
		public static final String END_TAG_KEY = "xmlinput.end";

		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context) {
			return new XmlRecordReader();
		}

		/**
		 * XMLRecordReader class to read through a given XML document to output
		 * XML blocks as records as specified by the start tag and end tag
		 *
		 */

		public static class XmlRecordReader extends
				RecordReader<LongWritable, Text> {
			private byte[] startTag;
			private byte[] endTag;
			private long start;
			private long end;
			private FSDataInputStream fsin;
			private DataOutputBuffer buffer = new DataOutputBuffer();

			private LongWritable key = new LongWritable();
			private Text value = new Text();

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
				endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
				FileSplit fileSplit = (FileSplit) split;

				// open the file and seek to the start of the split
				start = fileSplit.getStart();
				end = start + fileSplit.getLength();
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(conf);
				fsin = fs.open(fileSplit.getPath());
				fsin.seek(start);

			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if (fsin.getPos() < end) {
					if (readUntilMatch(startTag, false)) {
						try {
							buffer.write(startTag);
							if (readUntilMatch(endTag, true)) {
								key.set(fsin.getPos());
								value.set(buffer.getData(), 0,
										buffer.getLength());
								return true;
							}
						} finally {
							buffer.reset();
						}
					}
				}
				return false;
			}

			@Override
			public LongWritable getCurrentKey() throws IOException,
					InterruptedException {
				return key;
			}

			@Override
			public Text getCurrentValue() throws IOException,
					InterruptedException {
				return value;
			}

			@Override
			public void close() throws IOException {
				fsin.close();
			}

			@Override
			public float getProgress() throws IOException {
				return (fsin.getPos() - start) / (float) (end - start);
			}

			private boolean readUntilMatch(byte[] match, boolean withinBlock)
					throws IOException {
				int i = 0;
				while (true) {
					int b = fsin.read();
					// end of file:
					if (b == -1)
						return false;
					// save to buffer:
					if (withinBlock)
						buffer.write(b);
					// check if we're matching:
					if (b == match[i]) {
						i++;
						if (i >= match.length)
							return true;
					} else
						i = 0;
					// see if we've passed the stop point:
					if (!withinBlock && i == 0 && fsin.getPos() >= end)
						return false;
				}
			}
		}
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String document = value.toString();
			System.out.println("‘" + document + "‘");
			try {
				XMLStreamReader reader = XMLInputFactory.newInstance()
						.createXMLStreamReader(
								new ByteArrayInputStream(document.getBytes()));
				String propertyName = "";
				String propertyValue = "";
				String currentElement = "";
				while (reader.hasNext()) {
					int code = reader.next();
					switch (code) {
					case XMLStreamConstants.START_ELEMENT: // START_ELEMENT:
						currentElement = reader.getLocalName();
						break;
					case XMLStreamConstants.CHARACTERS: // CHARACTERS:
						if (currentElement.equalsIgnoreCase("ID")) {
							propertyName += reader.getText() + "\t";
							System.out.println("ID " + propertyName);
						} else if (currentElement.equalsIgnoreCase("NAME")) {
							propertyValue += reader.getText() + "\t";
							System.out.println("NAME " + propertyValue);
						} else if (currentElement.equalsIgnoreCase("DOJ")) {
							propertyValue += reader.getText() + "\t";
							System.out.println("DOJ " + propertyValue);
						} else if (currentElement.equalsIgnoreCase("DEPTID")) {
							propertyValue += reader.getText() + "\t";
							System.out.println("DEPT ID " + propertyValue);
						} else if (currentElement.equalsIgnoreCase("LOCATION")) {
							propertyValue += reader.getText();
							System.out.println("LOCATION " + propertyValue);
						}
						break;
					}
				}
				reader.close();
				context.write(new Text(propertyName.trim()), new Text(
						propertyValue.trim()));

			} catch (Exception e) {
				throw new IOException(e);

			}

		}
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new XmlDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<EMPLOYEE>");
		conf.set("xmlinput.end", "</EMPLOYEE>");
		Job job = Job.getInstance(conf, "XML Parsing Uing Map Reduce");
		job.setJarByClass(XmlDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(XmlDriver.Map.class);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println("XmlDriver.main() - input path " + args[0]);
		System.out.println("XmlDriver.main()  - output path " + args[1]);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}

/**
 * 
 * 
 <EMPLOYEES>
 * <EMPLOYEE><ID>1</ID><NAME>SRIRAM</NAME><DOJ>21-04-2016</DOJ><LOCATION
 * >AYODHYA</LOCATION></EMPLOYEE>
 * <EMPLOYEE><ID>2</ID><NAME>SEETA</NAME><DOJ>21-04
 * -2016</DOJ><LOCATION>MIDHILA</LOCATION></EMPLOYEE>
 * <EMPLOYEE><ID>3</ID><NAME>LAKSHMANA
 * </NAME><DOJ>21-03-2016</DOJ><LOCATION>AYODHYA</LOCATION></EMPLOYEE>
 * <EMPLOYEE>
 * <ID>4</ID><NAME>BHARATHA</NAME><DOJ>21-03-2016</DOJ><LOCATION>AYODHYA
 * </LOCATION></EMPLOYEE>
 * <EMPLOYEE><ID>5</ID><NAME>SETHRUGNA</NAME><DOJ>21-02-2016
 * </DOJ><LOCATION>AYODHYA</LOCATION></EMPLOYEE> </EMPLOYEES>
 */
