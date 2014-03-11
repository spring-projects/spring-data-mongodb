package org.springframework.data.mongodb.core.batch;

import java.util.List;

import org.springframework.stereotype.Repository;

/**
 * @author Guto Bortolozzo
 * see DATAMONGO-867
 */
@Repository
public interface BatchInsertOperations<T> {

	public void insert(T element);

	public void insert(List<T> elements);

	public void flush();

	public void clear();
	
	public int contentSize();
}