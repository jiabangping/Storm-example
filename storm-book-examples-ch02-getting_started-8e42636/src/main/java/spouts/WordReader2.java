package spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 喷嘴 实现了IRichSpot接口 WordReader负责读取文件并且将 每行提供给一个bolt螺栓
 * 一个spot发射一个定义的域的列表，允许有多种bolt读取相同的spot流 ,然后这些bolt定义的域提供给其他的bolt消费
 *
 */
public class WordReader2 extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private TopologyContext context;
	private FileReader fileReader;
	private boolean completed = false;
	@Override
	public void ack(Object msgId) {
		System.err.println("WordReader2.ack(),msgId="+msgId);
	}
	@Override
	public void close() {
		System.err.println("WordReader2.close()");
	}
	@Override
	public void fail(Object msgId) {
		System.err.println("WordReader2.fail(),FAIL:" + msgId);
	}

	/**第二个方法
	 * The only thing that the methods will do It is emit each file line
	 * 这个方法发射文件的 每一行 nextTuple元组/数据
	 */
	@Override
	public void nextTuple() {
		System.err.println("WordReader2.nextTuple()");
		/**
		 * 这个方法会一直被调用，所以会等着读完这个文件然后返回。
		 *  The nextuple it is called forever, so if
		 * we have been readed the file we will wait and then return
		 */
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// Do nothing
			}
			return;
		}
		String str;
		// Open the reader
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			// Read all lines
			while ((str = reader.readLine()) != null) {
				/**每行发射一个value
				 * By each line emmit a new value with the line as a their
				 */
				this.collector.emit(new Values(str), str);//每读一行文件 发射一个值
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			completed = true;
		}
	}

	/**
	 * 第一个方法   初始化操作
	 * We will create the file and get the collector object
	 * 在任何spot中第一个调用的方法都是open(Map conf,TopologyContext context,SpoutOutputCollector collector)
	 * @context 包含所有的拓扑数据
	 * @conf 在拓扑定义的时候被创建
	 * @collector 发射将被bolt处理的数据
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		System.err.println("WordReader2.open()");
		try {
			this.context = context;
			Object filePath = conf.get("wordsFile2");
			//创建reader负责读文件
			this.fileReader = new FileReader(filePath.toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["
					+ conf.get("wordFile") + "]");
		}
		this.collector = collector;
	}
	

	/**
	 * Declare the output field "word"
	 * 申明 output 域 
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.err.println("WordReader2.declareOutputFields()");
		Fields fields = new Fields("line");
		declarer.declare(fields);
	}
}
