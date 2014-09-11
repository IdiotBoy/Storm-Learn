package com.hulu.xuxin.directGroupingTest.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader extends BaseRichSpout {

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
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
		}
		
		BufferedReader reader = new BufferedReader(fileReader);
		try{
			String str;
			while((str = reader.readLine()) != null){
				this.collector.emit(new Values(str), str);
			}
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple", e);
		}finally{
			completed = true;
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file [" + conf.get("wordFile") + "]");
		}
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}
