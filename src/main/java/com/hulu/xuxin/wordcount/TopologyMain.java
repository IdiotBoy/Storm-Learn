package com.hulu.xuxin.wordcount;
import com.hulu.xuxin.wordcount.bolts.WordCounter;
import com.hulu.xuxin.wordcount.bolts.WordNormalizer;
import com.hulu.xuxin.wordcount.spouts.WordSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-spout", new WordSpout());
		builder.setBolt("word-normalizer", new WordNormalizer())
			   .shuffleGrouping("word-spout");
		builder.setBolt("word-counter", new WordCounter())
			   .fieldsGrouping("word-normalizer", new Fields("word"));
		
        //Configuration
		Config conf = new Config();
		conf.setDebug(false);
        //Topology run
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		Thread.sleep(1000000);
		//cluster.shutdown();
	}
}
