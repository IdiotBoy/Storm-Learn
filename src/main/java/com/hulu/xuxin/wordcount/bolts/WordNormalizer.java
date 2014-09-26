package com.hulu.xuxin.wordcount.bolts;

import java.util.Map;

import backtype.storm.daemon.acker;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer extends BaseRichBolt {
	
	OutputCollector _collector;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                _collector.emit(new Values(word));
            }
        }
        
        _collector.ack(input);
	}

	@Override
	public void prepare(Map paramMap, TopologyContext paramTopologyContext,
			OutputCollector paramOutputCollector) {
		_collector = paramOutputCollector;
	}
}
