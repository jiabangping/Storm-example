package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 第一个bolt 负责获取并标准化行，它将行分隔成单词，将单词转化成小写并且trim单词
 * 
 */
public class WordNormalizer extends BaseBasicBolt {
	@Override
	public void cleanup() {
		System.err.println("WordNormalizer.cleanup()");
	}

	/**
	 * The bolt will receive the line from the
	 * words file and process it to Normalize this line
	 * 
	 * The normalize will be put the words in lower case
	 * and split the line to get all words in this 
	 * 输入的元祖，在这里将被处理
	 * 
	 */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.err.println("WordNormalizer.execute()");
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                collector.emit(new Values(word,"1"));
            }
        }
	}
	
 
	/**
	 * The bolt will only emit the field "word" 
	 * 声明bolt的输出参数
	 * bolt将输出一个域，名为word
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.err.println("WordNormalizer.declareOutputFields()");
		declarer.declare(new Fields("word","counts"));
	}
}
