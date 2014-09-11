package com.hulu.xuxin.directGroupingTest.bolts;

import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer extends BaseBasicBolt {

	List<Integer> targetTasks;
	
	@Override
	public void cleanup() {}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                //collector.emit(new Values(word));
                System.out.println("WordNormalizer: " + getTargetTaskId(word));
                collector.emitDirect(getTargetTaskId(word), new Values(word));
            }
        }
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		targetTasks = context.getComponentTasks("word-counter");
	}
	

	
	private Integer getTargetTaskId(String word) {
		if (word.isEmpty())
			return targetTasks.get(0);
		else 
			return targetTasks.get(word.charAt(0) % targetTasks.size());
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word"));
	}

}
