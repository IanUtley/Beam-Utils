package org.ianutley.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelimitedTextSourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(DelimitedTextSourceTest.class);
    
    @Test
    public void testSimple() {
        String testfileLocation = this.getClass().getResource("/test1.csv").getFile();
        
        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        PCollection<TextRecord> data = p.apply("Read CSV",
            Read.from(DelimitedTextSource.withFilename(StaticValueProvider.of(testfileLocation))));

        PCollection<Long> output = data.apply(Count.globally()).apply("Result", MapElements.into(TypeDescriptor.of(Long.class)).via((Long l) -> {
            LOG.info(l.toString());
            return l;
        }));
        
        p.run().waitUntilFinish();
        PAssert.thatSingleton(output).isEqualTo(7L);
    }

}
