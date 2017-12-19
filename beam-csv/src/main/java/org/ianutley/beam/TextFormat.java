package org.ianutley.beam;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TextFormat {

	private boolean header = false;
	private String recordDelimiter = "\n";
	private Charset charset = StandardCharsets.UTF_8;
	
	public String getRecordDelimiter() {
		return recordDelimiter;
	}
	public void setRecordDelimiter(String recordDelimiter) {
		this.recordDelimiter = recordDelimiter;
	}
	public boolean isHeader() {
		return header;
	}
	public void setHeader(boolean header) {
		this.header = header;
	}
	public Charset getCharset() {
		return charset;
	}
	public void setCharset(Charset charset) {
		this.charset = charset;
	}

}
