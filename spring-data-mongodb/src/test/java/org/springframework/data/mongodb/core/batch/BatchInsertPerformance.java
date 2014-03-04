package org.springframework.data.mongodb.core.batch;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.StopWatch;
import org.springframework.util.StopWatch.TaskInfo;

/**
 * @author Joao Bortolozzo
 * see DATAMONGO-867
 */
public class BatchInsertPerformance extends AbstractBatchConfiguration{
	
	private final StopWatch watcher = new StopWatch();
	private static final String BATCH_INSERT_NAME = "BATCH INSERT"; 
	private static final String ONE_BY_ONE_INSERT_NAME = "ONE BY ONE INSERT"; 
	private static final String LIST_INSERT_NAME = "LIST INSERT"; 
	private static final int RANGE = 10000;
	private static final int BUCKET = 10;
	private static final int ELEMENTS_SHUFFLE = 30;
	
	@Autowired BatchInsertOperation<Person> batch;
	@Autowired MongoOperations template;
	
	@Test
	public void testBatchCollectionOfPerson(){
		
		Statistics statistics = new Statistics();
		
		for (ExecuteTask task : shuffleList()){
			task.execute(RANGE, BUCKET);
			statistics.set(task.name(), task.taskInfo());
		}
		
		System.out.println(statistics.getTaskStatistics());
		
		long people = template.count(new Query(Criteria.where("firstName").is("Joao")), Person.class);
		
		Long elements = RANGE * ELEMENTS_SHUFFLE * 3l;
		
		assertThat(people, is(elements));
	}

	public List<ExecuteTask> shuffleList(){
		
		List<ExecuteTask> tasks = new ArrayList<ExecuteTask>();
		
		for(int index = 0; index < ELEMENTS_SHUFFLE; index++){
			tasks.add(batchInsert());
			tasks.add(oneByOneInsert());
			tasks.add(listInsert());
		}
		
		Collections.shuffle(tasks);
		
		return tasks;
	}
	
	private ExecuteTask batchInsert() {
		ExecuteTask batchInsert = new ExecuteTask(){
			public void execute(int range, int bucketSize) {
				
				Map<Integer, List<Person>> people = new HashMap<Integer, List<Person>>();
				
				for(int index = 0; index < bucketSize; index++)
					people.put(index, populateCollection(range/bucketSize));
				
				watcher.start(BATCH_INSERT_NAME);
				
				for (Integer index : people.keySet())
					batch.insert(people.get(index));
				
				batch.flush();
				watcher.stop();
			};
			
			public TaskInfo taskInfo() {
				return watcher.getLastTaskInfo();
			}

			public String name() {
				return watcher.getLastTaskName();
			};
		};
		return batchInsert;
	}

	private ExecuteTask oneByOneInsert() {
		ExecuteTask oneByOneInsert = new ExecuteTask(){
			public void execute(int range, int bucketSize) {
				
				List<Person> peopleOneByOne = populateCollection(range);
				
				watcher.start(ONE_BY_ONE_INSERT_NAME);
				
				for (Person person : peopleOneByOne)
					template.insert(person);
				
				watcher.stop();
			};
			
			public TaskInfo taskInfo() {
				return watcher.getLastTaskInfo();
			}

			public String name() {
				return watcher.getLastTaskName();
			};
		};
		return oneByOneInsert;
	}

	private ExecuteTask listInsert() {
		ExecuteTask collectionInsert = new ExecuteTask(){
			public void execute(int range, int bucketSize) {
				
				List<Person> people = populateCollection(range);
				
				watcher.start(LIST_INSERT_NAME);
				
				template.insertAll(people);
				
				watcher.stop();
			}

			public TaskInfo taskInfo() {
				return watcher.getLastTaskInfo();
			}

			public String name() {
				return watcher.getLastTaskName();
			};
		};
		return collectionInsert;
	}

	static interface ExecuteTask {
		void execute(int range, int bucketSize);
		TaskInfo taskInfo();
		String name();
	}
	
	static class Statistics {
		
		private static final String SEPARATOR = "========================================================================\n";
		Map<String, List<TaskInfo>> tasks = new HashMap<String, List<TaskInfo>>();
		
		public Statistics(){
			tasks.put(BATCH_INSERT_NAME, new ArrayList<TaskInfo>());
			tasks.put(ONE_BY_ONE_INSERT_NAME, new ArrayList<TaskInfo>());
			tasks.put(LIST_INSERT_NAME, new ArrayList<TaskInfo>());
		}
		
		void set(String name, TaskInfo info){
			tasks.get(name).add(info);
		}
		
		String getTaskStatistics(){
			
			String information = SEPARATOR;
			
			for (String key : tasks.keySet()){
				Double averageTime = taskInformation(tasks.get(key));
				information+= key.toUpperCase()+" task performed in a mean time of "+averageTime+" ms\n";
				information += averageDifference(key, averageTime);
				information += SEPARATOR;
			}
			
			return information;
		}
		
		private String averageDifference(String currentKey, Double comparisonAverage){
			
			String comparison = "";
			
			for (String key : tasks.keySet()) {
				if(key.equals(currentKey)) continue;
				
				Double taskAverage = taskInformation(tasks.get(key));
				
				Double difference = calculaDifference(comparisonAverage, taskAverage);
				
				String qualifier = comparisonAverage < taskAverage ? "LESS" : "MORE";
				
				comparison += currentKey+" took "+difference+" ms "+qualifier + " than "+key+" to execute\n";
			}
			
			return comparison;
		}

		private Double calculaDifference(Double comparisonAverage, Double taskAverage) {
			
			double max = Math.max(comparisonAverage, taskAverage);
			double min = Math.min(comparisonAverage, taskAverage);
			
			return max - min;
		}
		
		private Double taskInformation(List<TaskInfo> informations){
			
			Double averageTime = 0d;
			
			for (TaskInfo taskInfo : informations)
				averageTime += taskInfo.getTimeMillis();
			
			return averageTime;
		}
	}
}
