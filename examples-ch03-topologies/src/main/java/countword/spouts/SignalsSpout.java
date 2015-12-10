package countword.spouts;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

//见第4章 All分组，如果你需要刷新缓存可以一个到所有的 发送信号
public class SignalsSpout extends BaseRichSpout{

	private SpoutOutputCollector collector;


	@Override
	public void nextTuple() {
		collector.emit("signals",new Values("refreshCache"));//streamId,tuple,messageId=null
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("signals",new Fields("action"));//声明一个streamId和字段
	}

}
