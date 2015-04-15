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
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.io.TextIO;

import com.google.cloud.dataflow.sdk.transforms.DoFn;



public class HelloWorldWithCloudDeployement {

    static class PrintTimestamps extends DoFn<String, String> {
      @Override
      public void processElement(ProcessContext c) {
	  c.output(c.element() + ":" + c.timestamp().getMillis());
	  System.out.println(c.element() + ":" + c.timestamp().getMillis());
      }
    }

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
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

    // Then create the pipeline.
    Pipeline p = Pipeline.create(options);

    long currentTimeMillis = System.currentTimeMillis();

    // Create data
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



    // Get a Data in a Pipeline
    PCollection<String> items = p.apply(Create.timestamped(data));

    // Create 250ms windows
    Window.Bound<String> window = Window.<String>into(FixedWindows.of(Duration.millis(250)));
    PCollection<String> fixed_windowed_items = items.apply(window);

    // Count elements in windows
    PCollection<KV<String, Long>> windowed_counts = fixed_windowed_items.apply(Count.<String>perElement());

    // Remove remove all data < 2
    PCollection<KV<String, Long>> windowed_filtered = windowed_counts.apply(ParDo.of(new FilterGreaterThan()));

    // Format for printing
    PCollection<String> windowed_outputString = windowed_filtered.apply(ParDo.of(new FormatCountsFn()));

    // Write to file
    String outputFile = GcsPath.fromUri("gs://dg-dataflow/").resolve("counts.txt").toString();
    windowed_outputString.apply(TextIO.Write.named("WriteResult").to(outputFile));


    System.out.println("\n\n\n");
    p.run();
    System.out.println("\n\n\n");

  }

}
