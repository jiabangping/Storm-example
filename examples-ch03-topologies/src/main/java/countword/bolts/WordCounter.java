package countword.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordCounter extends BaseRichBolt {

	Integer id;
	String name;
	Map<String, Integer> counters;
	private OutputCollector collector;

	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup() {
		System.out.println("-- Word Counter ["+name+"-"+id+"] --");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}
	}

	/**
	 * On each word We will count
	 */
	@Override
	public void execute(Tuple input) {
		
		if(input.getSourceStreamId().equals("signals")) {
			System.out.println();
		}
			String str = null; 
			try{
				str = input.getStringByField("word"); 
				
			}catch (IllegalArgumentException e) {
				//Do nothing
			}
			
			if(str!=null){
				/**
				 * If the word dosn't exist in the map we will create
				 * this, if not We will add 1 
				 */
				if(!counters.containsKey(str)){
					counters.put(str, 1);
				}else{
					Integer c = counters.get(str) + 1;
					counters.put(str, c);
				}
			}else{
		//增加一个条件来判断流的来源，如果不元组到一个命名的流，则流的默认名字为‘default’
	 //增加了另一个流到 word-counter 将元组从 signals-spout发送到这个bolt的所有实例
				if(input.getSourceStreamId().equals("signals")){
					str = input.getStringByField("action");
					if("refreshCache".equals(str))
						counters.clear();
				}
			}
		//Set the tuple as Acknowledge
		collector.ack(input);
	}

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
