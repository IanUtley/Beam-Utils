package org.ianutley.beam;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@DefaultCoder(SerializableCoder.class)
public class TextRecord implements Serializable {

	private static final long serialVersionUID = 1L;
	private List<String> fields = new ArrayList<>();
    
    public void add(String value) {
    	    fields.add(value);
    }
    
    public List<String> getTokens() {
    	    return Collections.unmodifiableList(fields);
    }
    
    @Override
    public String toString() {
    	    return fields.toString();
    }
}
