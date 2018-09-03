# flink-stream-processing-refarch

This repository contains the resources of the reference architecture for real-time stream processing with Apache Flink on Amazon EMR, Amazon Kinesis, and Amazon Elasticsearch Service that is discussed on the [AWS Big Data Blog](https://aws.amazon.com/blogs/big-data/build-a-real-time-stream-processing-pipeline-with-apache-flink-on-aws/).

The following example illustrates a scenario related to optimizing taxi fleet operations. You build a real-time stream processing pipeline that continuously receives information from a fleet of taxis operating in New York City. Using this data, you want to optimize the operations by analyzing the gathered data in real time and making data-based decisions.


## Building and running the reference architecture

To see the taxi trip analysis application in action, use two CloudFormation templates to build and run the reference architecture:

  -  The first template builds the runtime artifacts for ingesting taxi trips into the stream and for analyzing trips with Flink
  -  The second template creates the resources of the infrastructure that run the application

### Building the runtime artifacts and creating the infrastructure

Execute the [first CloudFormation template](cfn-templates/flink-refarch-build-artifacts.yml) to create an AWS CodePipeline pipeline, which builds the artifacts by means of AWS CodeBuild in a serverless fashion. You can also install Maven and building the Flink Amazon Kinesis connector and the other runtime artifacts manually. After all stages of the pipeline complete successfully, you can retrieve the artifacts from the S3 bucket that is specified in the output section of the CloudFormation template.

When the first template is created and the runtime artifacts are built, execute the [second CloudFormation template](cfn-templates/flink-refarch-infrastructure.yml), which creates the resources of the reference architecture described earlier.

Wait until both templates have been created successfully before proceeding to the next step. This takes up to 15 minutes, so feel free to get a fresh cup of coffee while CloudFormation does all the work for you.

### Starting the Flink runtime and submitting a Flink program

To start the Flink runtime and submit the Flink program that is doing the analysis, connect to the EMR master node. The parameters of this and later commands can be obtained from the output sections of the two CloudFormation templates, which have been used to provision the infrastructure and build the runtime artifacts.

```
$ ssh -C -D 8157 «EMR master node IP»
```

The EMR cluster that is provisioned by the CloudFormation template comes with two c4.xlarge core nodes with four vCPUs each. Generally, you match the number of node cores to the number of slots per task manager. For this post, it is reasonable to start a long-running Flink cluster with two task managers and four slots per task manager:

```
$ flink-yarn-session -n 2 -s 2 -jm 768 -tm 1024 -d
```

After the Flink runtime is up and running, the taxi stream processor program can be submitted to the Flink runtime to start the real-time analysis of the trip events in the Amazon Kinesis stream.

```
$ aws s3 cp s3://«artifact-bucket»/artifacts/flink-taxi-stream-processor-1.3.jar .

$ flink run -p 4 flink-taxi-stream-processor-1.3.jar --region «AWS region» --stream «Kinesis stream name» --es-endpoint https://«Elasticsearch endpoint» --checkpoint s3://«Checkpoint bucket»
```

Now that the Flink application is running, it is reading the incoming events from the stream, aggregating them in time windows according to the time of the events, and sending the results to Amazon ES. The Flink application takes care of batching records so as not to overload the Elasticsearch cluster with small requests and of [signing the batched requests](https://aws.amazon.com/blogs/database/set-access-control-for-amazon-elasticsearch-service/) to enable a secure configuration of the Elasticsearch cluster.

If you have activated a [proxy in your browser](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-proxy.html), you can [explore the Flink web interface](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-flink.html#w1ab1c48c37) through the dynamic port forwarding that has been established by the SSH session to the master node.

### Ingesting trip events into the Amazon Kinesis stream

To ingest the events, use the taxi stream producer application, which replays a historic dataset of taxi trips recorded in New York City from S3 into an Amazon Kinesis stream with eight shards. In addition to the taxi trips, the producer application also ingests watermark events into the stream so that the Flink application can determine the time up to which the producer has replayed the historic dataset.

```
$ ssh -C «producer instance IP»

$ aws s3 cp s3://«Artifact bucket»/artifacts/kinesis-taxi-stream-producer-1.3.jar .

$ java -jar kinesis-taxi-stream-producer-1.3.jar -speedup 6480 -stream «Kinesis stream name» -region «AWS region»
```

This application is by no means specific to the reference architecture discussed in this post. You can easily reuse it for other purposes as well, for example, building a similar stream processing architecture based on Amazon Kinesis Analytics instead of Apache Flink.

### Exploring the Kibana dashboard

Now that the entire pipeline is running, you can finally explore the Kibana dashboard that displays insights that are derived in real time by the Flink application:

```
https://«Elasticsearch end-point»/_plugin/kibana/app/kibana#/dashboard/Taxi-Trips-Dashboard
```

For the purpose of this post, the Elasticsearch cluster is configured to accept connections from the IP address range specified as a parameter of the CloudFormation template that creates the infrastructure. For production-ready applications, this may not always be desirable or possible. For more information about how to securely connect to your Elasticsearch cluster, see the [Set Access Control for Amazon Elasticsearch Service post](https://aws.amazon.com/blogs/database/set-access-control-for-amazon-elasticsearch-service/) on the AWS Database blog.

In the Kibana dashboard, the map on the left visualizes the start points of taxi trips. The redder a rectangle is, the more taxi trips started in that location. The line chart on the right visualizes the average duration of taxi trips to John F. Kennedy International Airport and LaGuardia Airport, respectively.

You can now scale the underlying infrastructure. For example, scale the shard capacity of the stream, change the instance count or the instance types of the Elasticsearch cluster, and verify that the entire pipeline remains functional and responsive even during the rescale operation.
