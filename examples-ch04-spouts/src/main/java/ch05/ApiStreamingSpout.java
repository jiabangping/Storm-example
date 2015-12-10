package ch05;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ApiStreamingSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<String>();
	static String redisHost = "127.0.0.1";
	static int redisPort = 6379;
	static Logger LOG = Logger.getLogger(ApiStreamingSpout.class);
	static JSONParser jsonParser = new JSONParser();

	@Override
	public void ack(Object msgId) {
		//确认成功了
	}

	@Override
	public void fail(Object msgId) {
		//失败，在允许的范围内重发，否则 停掉topology
	}
	
	@Override
	public void nextTuple() {
		try {
			while(! messages.isEmpty()) {
				collector.emit(new Values(messages.poll()));
			}
		} catch (Exception e) {
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						Jedis jedis = new Jedis(redisHost,redisPort);
						List<String> res = jedis.blpop(Integer.MAX_VALUE, "list");
						messages.offer(res.get(1));
					} catch (Exception e) {
						e.printStackTrace();
						try {
							TimeUnit.MINUTES.sleep(1);
						} catch (Exception e2) {
						}
					}
				}
			}
		}).start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明 输出默认 default stream 两个字段 criteria,tweet
		declarer.declare(new Fields("criteria", "tweet"));
	}

}
