package com.hulu.xuxin.directGroupingTest;
import com.hulu.xuxin.directGroupingTest.bolts.WordCounter;
import com.hulu.xuxin.directGroupingTest.bolts.WordNormalizer;
import com.hulu.xuxin.directGroupingTest.spouts.WordReader;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer())
			   .shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(), 2)
			   //.fieldsGrouping("word-normalizer", new Fields("word"));
				.directGrouping("word-normalizer");
		
        //Configuration
		Config conf = new Config();
		conf.put("wordsFile", args[0]);
		conf.setDebug(false);
        //Topology run
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		Thread.sleep(15000);
		cluster.shutdown();
	}
}
