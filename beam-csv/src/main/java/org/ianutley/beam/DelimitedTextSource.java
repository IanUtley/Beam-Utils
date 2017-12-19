package org.ianutley.beam;

import java.io.IOException;
import java.io.Serializable;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelimitedTextSource extends FileBasedSource<TextRecord> implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(DelimitedTextSource.class);
	private static final long serialVersionUID = 1L;
	private static enum ParseState { FIND_FIELD, PARSE_FIELD, DONE};
	private static long SPLIT_SIZE = 64*1024;
	
	private DelimitedTextFormat config = new DelimitedTextFormat();

	public static DelimitedTextSource withFilename(ValueProvider<String> spec) {
		return new DelimitedTextSource(spec, EmptyMatchTreatment.ALLOW_IF_WILDCARD, SPLIT_SIZE);
	}
	
	public DelimitedTextSource(ValueProvider<String> fileOrPatternSpec, EmptyMatchTreatment emptyMatchTreatment,
			long minBundleSize) {
		super(fileOrPatternSpec, emptyMatchTreatment, minBundleSize);
	}

	public DelimitedTextSource(Metadata fileMetadata, long minBundleSize, long startOffset, long endOffset) {
		super(fileMetadata, minBundleSize, startOffset, endOffset);
	}

	@Override
	public Coder<TextRecord> getOutputCoder() {
		return SerializableCoder.of(TextRecord.class);
	}

	
	@Override
	protected boolean isSplittable() throws Exception {
		return true;
	}


	public class DelimitedReader extends FileBasedReader<TextRecord> {
		private static final int BUFFER_SIZE = 16 * 1024;
		private char[] buf = new char[BUFFER_SIZE];
		private CharBuffer buffer = CharBuffer.wrap(buf);
		private java.io.Reader reader;
		private DelimitedTextSource textsource;
		private DelimitedTextFormat format;
		private long bufferStart=0L;
		
		public DelimitedReader(FileBasedSource<TextRecord> source) {
			super(source);
			format = ((DelimitedTextSource) source).config;
		}

		@Override
		protected void startReading(ReadableByteChannel channel) throws IOException {
			// Seek to start;

			boolean foundStart = false;
			this.textsource = (DelimitedTextSource) getCurrentSource();
			reader = Channels.newReader(channel, format.getCharset().newDecoder(), BUFFER_SIZE);

			LOG.info("Reading: [" + textsource.getStartOffset() + "-" + textsource.getEndOffset() + "]");
			buffer.clear();
			// First fill buffer with data
			reader.read(buffer);
			buffer.rewind();
			long startOffset = getLogicalOffset();
			if (startOffset == 0) {
				return;
			}
			consumeUntilRecordStart();
			if (!foundStart) {
				LOG.info("Could not find start of record.");
			}

			LOG.debug("New offset [" + textsource.getStartOffset() + "-" + textsource.getEndOffset() + "]@"
					+ getLogicalOffset());
		}
		
		private boolean eatMatch(String match) {
			int startIdx = buffer.position();
			int textLength = match.length();
			for(int idx = 0; idx< textLength; idx++) {
				if (buf[startIdx+idx] != match.charAt(idx)) return false;
			}
			buffer.position(startIdx + textLength);
			return true;
		}
		
		private void consumeUntilRecordStart() {
			while(true) {
			    if (getCharacter() == format.getRecordDelimiter().charAt(0)) {
			    	    buffer.position(buffer.position() - 1);
			    	    if (eatMatch(format.getRecordDelimiter())) break;
			    }
			}
		}
		
		private void eatWhitespace() {
			char c;
			while (true) {
				c = getCharacter();
				if (c != ' ') break;
			}
			buffer.position(buffer.position()-1);
		}

		@Override
		protected boolean readNextRecord() throws IOException {

			LOG.debug("Finding next record: [" + textsource.getStartOffset() + "-" + textsource.getEndOffset() + "]@" + getCurrentOffset());
			if (getLogicalOffset() == 0 && textsource.getStartOffset() == 0) {
				return true;
			} else if (getLogicalOffset() == 0){
				// Not sure.
				consumeUntilRecordStart();
			}
			long currentOffset = getLogicalOffset();
			LOG.debug("Returning next record position as: " + currentOffset);
			return (getCurrentOffset() < textsource.getEndOffset());
		}

		@Override
		protected long getCurrentOffset() throws NoSuchElementException {
			return getLogicalOffset() + textsource.getStartOffset();
		}

		@Override
		public TextRecord getCurrent() throws NoSuchElementException {
			long currentPosition = getCurrentOffset();
			ParseState state = ParseState.FIND_FIELD;
			TextRecord record = new TextRecord();
			LOG.debug("Parsing record: [" + textsource.getStartOffset() + "-" + textsource.getEndOffset() + "]@"
					+ currentPosition);
			char c;
			StringBuilder sb = null;
			boolean isQuoted = false;
			while (state != ParseState.DONE) {
				switch (state) {
				case FIND_FIELD: {
					eatWhitespace();
					if (eatMatch(format.getRecordDelimiter())) {
						state = ParseState.DONE;
						break;
					} else if (eatMatch(format.getFieldSeparator())) {
						record.add(null);
					} else {
						state = ParseState.PARSE_FIELD;
						sb = new StringBuilder();
						if (eatMatch(format.getFieldStartDelimiter())) {
							isQuoted = true;
						}
					}
					break;
				}
				case PARSE_FIELD: {
					if (!isQuoted) {
						while ((c = getCharacter()) != format.getFieldSeparator().charAt(0) && (c != format.getRecordDelimiter().charAt(0))) {
							sb.append(c);
						}
						if (c == format.getFieldSeparator().charAt(0) || c == format.getRecordDelimiter().charAt(0)) {
							buffer.position(buffer.position()-1);
						}
						if (eatMatch(format.getRecordDelimiter())) {
							record.add(sb.toString());
							state = ParseState.DONE;
						} else if (eatMatch(format.getFieldSeparator())){
							record.add(sb.toString());
							state = ParseState.FIND_FIELD;
						} else {
							// Consume as part of field value (continues parsing vale after fallthrough)
							c = getCharacter();
							sb.append(c);
						}
						
					} else { /*
						while ((c = getCharacter()) != format.getRecordDelimiter().charAt(0)) {
							if (c == format.getFieldStopDelimiter()) {
								c = getCharacter();
								// Escaped quote
								if (c == format.getFieldStopDelimiter()) {
									sb.append(c);
								} else {
									isQuoted = false;
									break;
								}
							} else {
								sb.append(c);
							}
						}
						record.add(sb.toString());
						while (((c = getCharacter()) != format.getRecordDelimiter().charAt(0))
								&& (c != format.getFieldSeparator())) {
							;
						}
						if (c == format.getRecordDelimiter().charAt(0)) {
							state = ParseState.DONE;
						} else {
							state = ParseState.FIND_FIELD;
						}
						*/
					}
					break;
				}
				case DONE:
				default:
					isQuoted = false;
					break;
				}
			}
			LOG.trace("Parsed record ended at " + getLogicalOffset());
			return record;
		}

		private long getLogicalOffset() {
			return bufferStart + buffer.position();
		}
		
		private char getCharacter() {
			if (buffer.position() >= buffer.limit()) {
                LOG.trace("Resizing buffer");
				try {
					bufferStart = bufferStart + buffer.limit();
					buffer.clear();
					reader.read(buffer);
					buffer.rewind();
					return buffer.get();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return buffer.get();
		}

	}

	@Override
	protected FileBasedSource<TextRecord> createForSubrangeOfFile(Metadata fileMetadata, long start, long end) {
		return new DelimitedTextSource(fileMetadata, end-start ,start, end);
	}

	@Override
	protected FileBasedReader<TextRecord> createSingleFileReader(PipelineOptions options) {
		return new DelimitedReader(this);
	}
}
