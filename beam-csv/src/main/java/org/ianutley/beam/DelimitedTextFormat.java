package org.ianutley.beam;

import java.io.Serializable;

public class DelimitedTextFormat extends TextFormat implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String fieldDelimiter = "\"";
	private String fieldStartDelimiter = "\"";
	private String fieldStopDelimiter = "\"";
	private String fieldSeparator = ",";

	public String getFieldDelimiter() {
		return fieldDelimiter;
	}

	public void setFieldDelimiter(String fieldDelimiter) {
		this.fieldDelimiter = fieldDelimiter;
		this.fieldStartDelimiter = fieldDelimiter;
		this.fieldStopDelimiter = fieldDelimiter;
	}

	public String getFieldStartDelimiter() {
		return fieldStartDelimiter;
	}

	public void setFieldStartDelimiter(String fieldStartDelimiter) {
		this.fieldStartDelimiter = fieldStartDelimiter;
	}

	public String getFieldStopDelimiter() {
		return fieldStopDelimiter;
	}

	public void setFieldStopDelimiter(String fieldStopDelimiter) {
		this.fieldStopDelimiter = fieldStopDelimiter;
	}

	public String getFieldSeparator() {
		return fieldSeparator;
	}

	public void setFieldSeparator(String fieldSeparator) {
		this.fieldSeparator = fieldSeparator;
	}
}
