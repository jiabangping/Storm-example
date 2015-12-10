package banktransactions;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TransactionsSpouts extends BaseRichSpout{

	private static final Integer MAX_FAILS = 2;
	Map<Integer,String> messages;//transactionId,transactionMessage
	Map<Integer,Integer> transactionFailureCount;
	Map<Integer,String> toSend;
	private SpoutOutputCollector collector;  
	
	static Logger LOG = Logger.getLogger(TransactionsSpouts.class);
	

	public void ack(Object msgId) {
		messages.remove(msgId);
		LOG.info("Message fully processed ["+msgId+"]");
	}

	public void close() {
		
	}

	public void fail(Object msgId) {
		if(!transactionFailureCount.containsKey(msgId))
			throw new RuntimeException("Error, transaction id not found ["+msgId+"]");
		Integer transactionId = (Integer) msgId;
		
		//Get the transactions fail
		Integer failures = transactionFailureCount.get(transactionId) + 1;
		if(failures >= MAX_FAILS){
			//If exceeds the max fails will go down the topology
			throw new RuntimeException("Error, transaction id ["+transactionId+"] has had many errors ["+failures+"]");
		}
		//If not exceeds the max fails we save the new fails quantity and re-send the message 
		transactionFailureCount.put(transactionId, failures);
		toSend.put(transactionId,messages.get(transactionId));
		LOG.info("Re-sending message ["+msgId+"]");
	}

	public void nextTuple() {
		/** 这里的逻辑是 判断如果 toSend 这个Map不为空，则遍历此map将数据全发射出去，然后将此Map清空
		 *  如果 bolt消费者中，处理成功调用了 ack()方法,则会调用spout这里的ack方法
		 *  如果bolt中调用了fail()方法 会调到  spout这里的 fail() 方法，可以在fail()方法中做重发处理，如果判断失败次数过多，则终端topology
		 */
		if(!toSend.isEmpty()){
			for(Map.Entry<Integer, String> transactionEntry : toSend.entrySet()){
				Integer transactionId = transactionEntry.getKey();
				String transactionMessage = transactionEntry.getValue();
				collector.emit(new Values(transactionMessage),transactionId);
			}
			/*
			 * The nextTuple, ack and fail methods run in the same loop, so
			 * we can considerate the clear method atomic
			 */
			toSend.clear();
		}
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {e.printStackTrace();}
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		Random random = new Random();
		messages = new HashMap<Integer, String>();
		toSend = new HashMap<Integer, String>();
		transactionFailureCount = new HashMap<Integer, Integer>();
		for(int i = 0; i< 1; i++){
			messages.put(i, "transaction_"+random.nextInt());
			transactionFailureCount.put(i, 0);
		}
		//初始化工作， messages中100个 <0,transaction_xxxx>, <1,transaction_xxxxx>,
		//transactionFailureCount中 <0,0> <1,0>...
		toSend.putAll(messages);
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transactionMessage"));
	}

}
