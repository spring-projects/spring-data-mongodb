/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb.convert;

import static org.springframework.data.mapping.MappingBeanHelper.isSimpleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public abstract class AbstractMongoConverter implements MongoConverter {

	public Object maybeConvertObject(Object obj) {
		if (obj instanceof Enum<?>) {
			return ((Enum<?>) obj).name();
		}

		if (null != obj && isSimpleType(obj.getClass())) {
			// Doesn't need conversion
			return obj;
		}

		if (obj instanceof BasicDBList) {
			return maybeConvertList((BasicDBList) obj);
		}

		if (obj instanceof DBObject) {
			DBObject newValueDbo = new BasicDBObject();
			for (String vk : ((DBObject) obj).keySet()) {
				Object o = ((DBObject) obj).get(vk);
				newValueDbo.put(vk, maybeConvertObject(o));
			}
			return newValueDbo;
		}

		if (obj instanceof Map) {
			Map<Object, Object> m = new HashMap<Object, Object>();
			for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) obj).entrySet()) {
				m.put(entry.getKey(), maybeConvertObject(entry.getValue()));
			}
			return m;
		}

		if (obj instanceof List) {
			List<?> l = (List<?>) obj;
			List<Object> newList = new ArrayList<Object>();
			for (Object o : l) {
				newList.add(maybeConvertObject(o));
			}
			return newList;
		}

		if (obj.getClass().isArray()) {
			return maybeConvertArray((Object[]) obj);
		}

		DBObject newDbo = new BasicDBObject();
		this.write(obj, newDbo);
		return newDbo;
	}

	public Object[] maybeConvertArray(Object[] src) {
		Object[] newArr = new Object[src.length];
		for (int i = 0; i < src.length; i++) {
			newArr[i] = maybeConvertObject(src[i]);
		}
		return newArr;
	}

	public BasicDBList maybeConvertList(BasicDBList dbl) {
		BasicDBList newDbl = new BasicDBList();
		Iterator iter = dbl.iterator();
		while (iter.hasNext()) {
			Object o = iter.next();
			newDbl.add(maybeConvertObject(o));
		}
		return newDbl;
	}

}
