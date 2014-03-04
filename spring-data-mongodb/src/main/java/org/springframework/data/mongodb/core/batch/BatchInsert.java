package org.springframework.data.mongodb.core.batch;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;

/**
 * @author Joao Bortolozzo
 * see DATAMONGO-867
 */
public final class BatchInsert implements BatchInsertOperations{
	
	@Autowired private MongoOperations template;
	
	private List<Object> batchContent = new ArrayList<Object>();
	
	private Integer batchSize = 0;

	public BatchInsert(Integer batchSize) {
		this.batchSize = batchSize;
	}
	
	private boolean shouldFlush(){
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
	public void insert(Object element){
		batchContent.add(element);
		if(shouldFlush()) flush();
	}
	
	@Override
	public void insertAll(List<? extends Object> elements){
		batchContent.addAll(elements);
		if(shouldFlush()) flush();
	}
}
