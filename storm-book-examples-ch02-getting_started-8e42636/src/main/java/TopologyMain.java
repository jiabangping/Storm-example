import spouts.WordReader;
import spouts.WordReader2;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import bolts.WordCounter;
import bolts.WordNormalizer;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		SpoutDeclarer spoutDeclar = builder.setSpout("word-reader",new WordReader());
		SpoutDeclarer spoutDeclar2 = builder.setSpout("word-reader2",new WordReader2());
		
		BoltDeclarer normalizerDeclarer = builder.setBolt("word-normalizer", new WordNormalizer());
		
		normalizerDeclarer.shuffleGrouping("word-reader");//此bolt从word-reader获取数据
		normalizerDeclarer.shuffleGrouping("word-reader2");
		
		//builder.setBolt("word-counter", new WordCounter(),2).fieldsGrouping("word-normalizer", new Fields("word")); 相同的单词送到相同的word-count
		
		
		builder.setBolt("word-counter", new WordCounter(),2).shuffleGrouping("word-normalizer");//使用shuffleGrouping以随机分布的方式将每条消息发给bolt
		
		
        //Configuration
		Config conf = new Config();
		conf.put("wordsFile", "src/main/resources/words.txt");
		conf.put("wordsFile2", "src/main/resources/words2.txt");
		conf.setDebug(true);
		
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
	/*	Thread.sleep(2000);
		cluster.shutdown();*/
	}
}
