package com.hulu.xuxin.wordcount.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Random;

import org.apache.commons.collections.map.StaticBucketMap;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WordSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	
	@Override
	public void ack(Object msgId) {
		System.out.println("OK: " + msgId);
	}
	
	@Override
	public void close() {}
	
	@Override
	public void fail(Object msgId) {
		System.out.println("FAIL: " + msgId);
	}

	@Override
	public void nextTuple() {
		Utils.sleep(10);
		String[] words = { "nathan", "mike", "jackson", "golda", "bertels" };
		Random rand = new Random();
		String word = words[rand.nextInt(words.length)];
		this.collector.emit(new Values(new Object[] { word }));
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}
