~/softs/apache-maven-3.1.0/bin/mvn clean install exec:java -Dexec.mainClass=HelloWorld
~/softs/apache-maven-3.1.0/bin/mvn  clean install exec:java -Dexec.mainClass=HelloWorldWithOptions -Dexec.args=" --project=deuxmilledollars  --runner=BlockingDataflowPipelineRunner --stagingLocation=gs://dg-dataflow/test/staging"

The introduction to Reactive Programming you've been missing
https://gist.github.com/staltz/868e7e9bc2a7b8c1f754


