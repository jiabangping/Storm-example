package bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordCounter extends BaseBasicBolt {

	Integer id;
	String name;
	Map<String, Integer> counters;

	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup() {
		System.err.println("--WordCounter.cleanup()  ["+name+"-"+id+"] --");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}
	}

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		System.err.println("--WordCounter.prepare()");
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.err.println("--WordCounter.declareOutputFields()");
	}


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.err.println("--WordCounter.execute()");
		String str = input.getString(0);
		String str2 = input.getStringByField("counts");
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
	}
}
