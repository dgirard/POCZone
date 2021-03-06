import com.google.cloud.dataflow.sdk.*;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.coders.*;
import com.google.cloud.dataflow.sdk.values.*;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.*;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.joda.time.Duration;
import java.util.List;
import java.util.Arrays;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

public class HelloWorld {

    static class FormatCountsFn extends DoFn<KV<String, Long>, String> {
	private static final long serialVersionUID = 0;

      @Override
      public void processElement(ProcessContext c) {
	String output = "Element: " + c.element().getKey()
	    + " Value: " + c.element().getValue()
	    + " Timestamp: " + c.timestamp();
	c.output(output);
        System.out.println(output);
      }
    }

    static class FilterGreaterThan extends DoFn<KV<String, Long>, KV<String,Long>> {
	private static final long serialVersionUID = 0;

      @Override
      public void processElement(ProcessContext c) {

	  if(c.element().getValue()>=2){
	      c.output(c.element());
	  }
      }
    }

    public static void main(String[] args) {

    // Start by defining the options for the pipeline.
    PipelineOptions options = PipelineOptionsFactory.create();

    // Then create the pipeline.
    Pipeline p = Pipeline.create(options);

    long currentTimeMillis = System.currentTimeMillis();

    List<TimestampedValue<String>> data = Arrays.asList(
	TimestampedValue.of("b", new Instant(currentTimeMillis)),
	TimestampedValue.of("b", new Instant(currentTimeMillis+251)),
	TimestampedValue.of("b", new Instant(currentTimeMillis+253)),
	TimestampedValue.of("b", new Instant(currentTimeMillis+501)),
	TimestampedValue.of("b", new Instant(currentTimeMillis+770)),
	TimestampedValue.of("b", new Instant(currentTimeMillis+774)),
	TimestampedValue.of("b", new Instant(currentTimeMillis+778)),
	TimestampedValue.of("b", new Instant(currentTimeMillis+1780)),
	TimestampedValue.of("b", new Instant(currentTimeMillis+11756)));


    PCollection<String> items = p.apply(Create.timestamped(data));
    PCollection<String> fixed_windowed_items =
       items.apply(Window.<String>into(FixedWindows.of(Duration.millis(250))));
    PCollection<KV<String, Long>> windowed_counts = fixed_windowed_items.apply(Count.<String>perElement());
    PCollection<KV<String, Long>> windowed_filtered = windowed_counts.apply(ParDo.of(new FilterGreaterThan()));
    windowed_filtered.apply(ParDo.of(new FormatCountsFn()));

    p.run();

}

}
