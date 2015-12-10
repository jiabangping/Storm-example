package countword;

import countword.spouts.SignalsSpout;
import countword.spouts.WordReader;
import countword.bolts.WordCounter;
import countword.bolts.WordNormalizer;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
		
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader",new WordReader());
		builder.setSpout("signals-spout",new SignalsSpout());
		
		builder.setBolt("word-normalizer", new WordNormalizer())
			.shuffleGrouping("word-reader");//以shuffleGrouping 流分组的方式
		
		//这里体现出了 每个InputDeclare可以有不止一个源，同时每个源可以用不同的流分组来分组 
		builder.setBolt("word-counter", new WordCounter(),2)
			.fieldsGrouping("word-normalizer",new Fields("word"))//使用
			.allGrouping("signals-spout","signals");//allGrouping 
			//多个源，每个源使用不同的流分组
		
        //Configuration
		Config conf = new Config();
		conf.put("wordsFile", "src/main/resources/words.txt");
		conf.setDebug(true);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Count-Word-Toplogy-With-Refresh-Cache", conf, builder.createTopology());
	/*	Thread.sleep(5000);
		cluster.shutdown();*/
	}
}
