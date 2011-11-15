package org.springframework.data.mongodb.config;

import com.mongodb.WriteConcern;

public class MyWriteConcern {
	
	public MyWriteConcern(WriteConcern wc) {
		this._w = wc.getWObject();
		this._continueOnErrorForInsert = wc.getContinueOnErrorForInsert();
		this._fsync = wc.getFsync();
		this._j = wc.getJ();
		this._wtimeout = wc.getWtimeout();
	}

  Object _w = 0;
  int _wtimeout = 0;
  boolean _fsync = false;
  boolean _j = false;
  boolean _continueOnErrorForInsert = false;
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (_continueOnErrorForInsert ? 1231 : 1237);
		result = prime * result + (_fsync ? 1231 : 1237);
		result = prime * result + (_j ? 1231 : 1237);
		result = prime * result + ((_w == null) ? 0 : _w.hashCode());
		result = prime * result + _wtimeout;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MyWriteConcern other = (MyWriteConcern) obj;
		if (_continueOnErrorForInsert != other._continueOnErrorForInsert)
			return false;
		if (_fsync != other._fsync)
			return false;
		if (_j != other._j)
			return false;
		if (_w == null) {
			if (other._w != null)
				return false;
		} else if (!_w.equals(other._w))
			return false;
		if (_wtimeout != other._wtimeout)
			return false;
		return true;
	}
  
}
