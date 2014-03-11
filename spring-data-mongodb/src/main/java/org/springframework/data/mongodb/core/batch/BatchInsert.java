package org.springframework.data.mongodb.core.batch;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;

/**
 * @author Guto Bortolozzo
 * see DATAMONGO-867
 */
public class BatchInsert<T> implements BatchInsertOperations<T>{

	@Autowired private MongoOperations template;

	private List<T> batchContent = new ArrayList<T>();

	private Integer batchSize = 0;

	protected BatchInsert(Integer batchSize) {
		this.batchSize = batchSize;
	}

	protected boolean mustFlush(){
		return batchContent.size() >= batchSize;
	}

	@Override
	public void flush() {
		template.insertAll(batchContent);
		clear();
	}

	@Override
	public void clear() {
		batchContent.clear();
	}

	@Override
	public void insert(T element){
		batchContent.add(element);
		if(mustFlush()) flush();
	}

	@Override
	public void insert(List<T> elements){
		batchContent.addAll(elements);
		if(mustFlush()) flush();
	}

	@Override
	public int contentSize() {
		return batchContent.size();
	}
}