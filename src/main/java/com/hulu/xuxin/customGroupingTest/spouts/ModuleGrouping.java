package com.hulu.xuxin.customGroupingTest.spouts;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class ModuleGrouping implements CustomStreamGrouping, Serializable {
	
	private List<Integer> targetTasks; 
	
	@Override
	public List<Integer> chooseTasks(int arg0, List<Object> values) { 
		System.out.print("Module Grouping: numTasks = " + targetTasks.size() + "\t");
		List<Integer> boltIds = new ArrayList<Integer>(); 
		if(values.size() > 0) {
			String str = values.get(0).toString(); 
			if(str.isEmpty()) {
				boltIds.add(targetTasks.get(0));
				System.out.println("empty: " + targetTasks.get(0));
			}
			else {
				boltIds.add(targetTasks.get(str.charAt(0) % targetTasks.size()));
				System.out.println("noempty " + targetTasks.get(str.charAt(0) % targetTasks.size()));
			}
		}
		//System.out.print("Module Grouping: ");
		//for (int i = 0; i < boltIds.size(); i++)
		//	System.out.print(i + "\t");
		//System.out.println();
		return boltIds;
	}

	@Override
	public void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1,
			List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
	}

}