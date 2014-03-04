package org.springframework.data.mongodb.core.batch;

import java.util.List;

/**
 * @author Joao Bortolozzo
 * see DATAMONGO-867
 */
public class BatchInsert<T> extends AbstractBatch<T> implements BatchInsertOperation<T>{

	protected BatchInsert(Integer batchSize) {
		super(batchSize);
	}

	@Override
	public void insert(T element) {
		super.elements.add(element);
		if(super.mustFlush()) flush();
	}

	@Override
	public void insert(List<T> elements) {
		super.elements.addAll(elements);
		if(super.mustFlush()) flush();
	}
	
	@Override
	public void flush() {
		template.insertAll(elements);
		clear();
	}
	
	@Override
	public void clear() {
		elements.clear();
	}
}
