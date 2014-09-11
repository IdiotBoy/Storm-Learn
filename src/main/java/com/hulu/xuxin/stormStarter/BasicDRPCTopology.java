package com.hulu.xuxin.stormStarter;

import java.security.InvalidParameterException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BasicDRPCTopology {
  public static class ExclaimBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String[] numbers = tuple.getString(1).split("\\+");
        Integer added = 0;
        if(numbers.length<2){
            throw new InvalidParameterException("Should be at least 2 numbers");
        }
        for(String num : numbers){
            added += Integer.parseInt(num);
        }
        collector.emit(new Values(tuple.getValue(0), added));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "result"));
    }
  }

  public static void main(String[] args) throws Exception {
	LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("add");
    builder.addBolt(new ExclaimBolt(), 3);

    Config conf = new Config();
    conf.setDebug(false);
    
    if (args == null || args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();

      cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));

      String result = drpc.execute("add", "1+-1");
      System.out.println("drpc: 1+-1 = " + result);
      result = drpc.execute("add", "1+1+5+10");
      System.out.println("drpc: 1+1+5+10 = " + result);
      
      cluster.shutdown();
      drpc.shutdown();
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createRemoteTopology());
    }
  }
}
