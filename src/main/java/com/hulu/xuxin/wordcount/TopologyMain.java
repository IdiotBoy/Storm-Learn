package com.hulu.xuxin.wordcount;
import com.hulu.xuxin.wordcount.bolts.WordCounter;
import com.hulu.xuxin.wordcount.bolts.WordNormalizer;
import com.hulu.xuxin.wordcount.spouts.WordReader;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer())
			   .shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(), 2)
			   .fieldsGrouping("word-normalizer", new Fields("word"));
		
        //Configuration
		Config conf = new Config();
		conf.put("wordsFile", args[0]);
		conf.setDebug(false);
        //Topology run
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		Thread.sleep(5000);
		cluster.shutdown();
	}
}