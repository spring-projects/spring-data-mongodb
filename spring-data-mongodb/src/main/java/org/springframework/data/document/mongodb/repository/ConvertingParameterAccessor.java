/*
 * Copyright 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.document.mongodb.repository;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.data.document.mongodb.MongoWriter;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;

/**
 * Custom {@link ParameterAccessor} that uses a {@link MongoWriter} to serialize parameters into Mongo format.
 *
 * @author Oliver Gierke
 */
public class ConvertingParameterAccessor implements ParameterAccessor {
  
  private final MongoWriter<Object> writer;
  private final ParameterAccessor delegate;

  /**
   * Creates a new {@link ConvertingParameterAccessor} with the given {@link MongoWriter} and delegate.
   *
   * @param writer
   */
  public ConvertingParameterAccessor(MongoWriter<Object> writer, ParameterAccessor delegate) {
    this.writer = writer;
    this.delegate = delegate;
  }

  /*
    * (non-Javadoc)
    *
    * @see java.lang.Iterable#iterator()
    */
  public Iterator<Object> iterator() {
    return new ConvertingIterator(delegate.iterator());
  }

  /*
    * (non-Javadoc)
    *
    * @see org.springframework.data.repository.query.ParameterAccessor#getPageable()
    */
  public Pageable getPageable() {
    return delegate.getPageable();
  }

  /*
    * (non-Javadoc)
    *
    * @see org.springframework.data.repository.query.ParameterAccessor#getSort()
    */
  public Sort getSort() {
    return delegate.getSort();
  }

  /* (non-Javadoc)
    * @see org.springframework.data.repository.query.ParameterAccessor#getBindableParameter(int)
    */
  public Object getBindableValue(int index) {

    return getConvertedValue(delegate.getBindableValue(index));
  }

  /**
   * Converts the given value with the underlying {@link MongoWriter}.
   *
   * @param value
   * @return
   */
  private Object getConvertedValue(Object value) {
    
    DBObject result = new BasicDBObject();
    writer.write(new ValueHolder(value), result);
    return ((DBObject) result.get("value")).get("value");
  }

  /**
   * Custom {@link Iterator} to convert items before returning them.
   *
   * @author Oliver Gierke
   */
  private class ConvertingIterator implements PotentiallyConvertingIterator {

    private final Iterator<Object> delegate;

    /**
     * Creates a new {@link ConvertingIterator} for the given delegate.
     *
     * @param delegate
     */
    public ConvertingIterator(Iterator<Object> delegate) {
      this.delegate = delegate;
    }

    /*
       * (non-Javadoc)
       *
       * @see java.util.Iterator#hasNext()
       */
    public boolean hasNext() {
      return delegate.hasNext();
    }

    /*
       * (non-Javadoc)
       *
       * @see java.util.Iterator#next()
       */
    public Object next() {

      return delegate.next();
    }
    
    /* (non-Javadoc)
     * @see org.springframework.data.document.mongodb.repository.ConvertingParameterAccessor.PotentiallConvertingIterator#nextConverted()
     */
    public Object nextConverted() {
      
      return getConvertedValue(next());
    }

    /*
       * (non-Javadoc)
       *
       * @see java.util.Iterator#remove()
       */
    public void remove() {
      delegate.remove();
    }
  }

  /**
   * Simple value holder class to allow conversion and accessing the converted value in a deterministic way.
   *
   * @author Oliver Gierke
   */
  private static class ValueHolder {

    private Map<String, Object> value = new HashMap<String, Object>();

    public ValueHolder(Object value) {

      this.value.put("value", value);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> getValue() {

      return value;
    }
  }

  /**
   * Custom {@link Iterator} that adds a method to access elements in a converted manner.
   *
   * @author Oliver Gierke
   */
  public interface PotentiallyConvertingIterator extends Iterator<Object> {
    
    /**
     * Returns the next element which has already been converted.
     * 
     * @return
     */
    Object nextConverted();
  }
}
