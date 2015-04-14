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
import java.util.ArrayList;
import java.util.Arrays;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.io.TextIO;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;



public class HelloWorldBigQuery10000 {

    static class PrintTimestamps extends DoFn<String, String> {
      @Override
      public void processElement(ProcessContext c) {
	  c.output(c.element() + ":" + c.timestamp().getMillis() + c.windows());
	  System.out.println(c.element() + ":" + c.timestamp().getMillis() + c.windows());
      }
    }


    // Format for text File
    static class FormatCountsFn extends DoFn<KV<String, Long>, String> {
	private static final long serialVersionUID = 0;

      @Override
      public void processElement(ProcessContext c) {
	String output = "Id: " + c.element().getKey()
	    + " / NbClicks: " + c.element().getValue()
	    + " / Timestamp: " + c.timestamp();
	    //	    + " Window: (" + c.windows() 
	    //	    + ")";
	c.output(output);
	//        System.out.println(c.element() + ":" + c.timestamp().getMillis() + c.windows());

        System.out.println(output);

      }
    }

    // Format for BigQuery
    static class FormatBigQuery extends DoFn<KV<String, Long>, TableRow> {
	private static final long serialVersionUID = 0;

    @Override
	public void processElement(ProcessContext c) {
	TableRow row = new TableRow()
	    .set("Id", c.element().getKey())
	    .set("Count", c.element().getValue().longValue())
	    .set("TimeStamp", ""+c.timestamp());
	c.output(row);
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
    List<TimestampedValue<String>> data = new ArrayList<TimestampedValue<String>>();
    for(int i=0; i<10000;i++)
	data.add(TimestampedValue.of("b", new Instant(currentTimeMillis+i)));

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

    // Build the table schema for the output table.                                                                                                                                  
    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
    fields.add(new TableFieldSchema().setName("Id").setType("STRING"));
    fields.add(new TableFieldSchema().setName("Count").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("TimeStamp").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);
    windowed_filtered.apply(ParDo.of(new FormatBigQuery()))
        .apply(BigQueryIO.Write
	   .to("deuxmilledollars:temp.res")
	   .withSchema(schema)
    	   .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	   .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));




    System.out.println("\n\n\n");
    p.run();
    System.out.println("\n\n\n");

  }

}
