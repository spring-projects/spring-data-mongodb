package org.springframework.data.document.mongodb;

import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class FluentDbObject {

	public static DBObject DbObject(Tuple... entries) {		
        BasicDBObject map = new BasicDBObject();

        for (Tuple entry : entries) {
            map.put(entry.t1, entry.t2);
        }
        
        return map;
    }

    public static Tuple pair(String o1, Object o2) {
        return new Tuple(o1, o2);
    }

    public static class Tuple {
        private String t1;
        private Object t2;

        public Tuple(String t1, Object t2) {
            this.t1 = t1;
            this.t2 = t2;
        }
    }

}
