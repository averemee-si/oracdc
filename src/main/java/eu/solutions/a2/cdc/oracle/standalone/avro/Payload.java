/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package eu.solutions.a2.cdc.oracle.standalone.avro;

import java.io.Serializable;
import java.util.Map;

public class Payload implements Serializable {

	private static final long serialVersionUID = -3059471472954936036L;

	private Map<String, Object> before;
	private Map<String, Object> after;
	private Source source;
	private String op;
	private long ts_ms;

	public Payload() {}

	public Payload(final Source source, final String op) {
		this.source = source;
		this.op = op;
	}

	public Map<String, Object> getBefore() {
		return before;
	}
	public void setBefore(Map<String, Object> before) {
		this.before = before;
	}

	public Map<String, Object> getAfter() {
		return after;
	}
	public void setAfter(Map<String, Object> after) {
		this.after = after;
	}

	public Source getSource() {
		return source;
	}
	public void setSource(Source source) {
		this.source = source;
	}

	public String getOp() {
		return op;
	}
	public void setOp(String op) {
		this.op = op;
	}

	public long getTs_ms() {
		return ts_ms;
	}
	public void setTs_ms(long ts_ms) {
		this.ts_ms = ts_ms;
	}

}
