package com.hulu.xuxin.wordcount.bolts;

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
	Map<String, Integer> wordsMap;

	@Override
	public void cleanup() {
		System.out.println("-- Word Counter [" + name + "-" + id + "] --");
		for(Map.Entry<String, Integer> entry : wordsMap.entrySet()){
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.wordsMap = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		if (!wordsMap.containsKey(str)) {
			wordsMap.put(str, 1);
		} else {
			Integer c = wordsMap.get(str) + 1;
			wordsMap.put(str, c);
		}
	}
}
