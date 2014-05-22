spark-jobserver provides a RESTful interface for submitting and managing [Apache Spark](http://spark-project.org) jobs, jars, and job contexts.
This repo contains the complete Spark job server project, including unit tests and deploy scripts.

We deploy our job server off of this repo at Ooyala and it is tested against CDH5 / Hadoop 2.2,  Spark 0.9.0 final, Scala 2.10.

## Features

- *"Spark as a Service"*: Simple REST interface for all aspects of job, context management
- Supports sub-second low-latency jobs via long-running job contexts
- Start and stop job contexts for RDD sharing and low-latency jobs; change resources on restart
- Kill running jobs via stop context
- Separate jar uploading step for faster job startup
- Asynchronous and synchronous job API.  Synchronous API is great for low latency jobs!
- Works with Standalone Spark as well as Mesos
- Job and jar info is persisted via a pluggable DAO interface

## Quick start / development mode

You need to have [SBT](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html) installed.

From SBT shell, simply type "re-start".  This uses a default configuration file.  An optional argument is a
path to an alternative config file.  You can also specify JVM parameters after "---".  Including all the
options looks like this:

    re-start /path/to/my.conf --- -Xmx8g

Note that re-start (SBT Revolver) forks the job server in a separate process.  If you make a code change, simply
type re-start again at the SBT shell prompt, it will compile your changes and restart the jobserver.  It enables
very fast turnaround cycles.

For example jobs see the job-server-tests/ project / folder.

When you use `re-start`, the log file goes to `job-server/job-server-local.log`.  There is also an environment variable
EXTRA_JAR for adding a jar to the classpath.

### WordCountExample walk-through

First, to package the test jar containing the WordCountExample: `sbt job-server-tests/package`.
Then go ahead and start the job server using the instructions above.

Let's upload the jar:

    curl --data-binary @job-server-tests/target/job-server-tests-0.3.0.jar localhost:8090/jars/test
    OK⏎

The above jar is uploaded as app `test`.  Next, let's start an ad-hoc word count job, meaning that the job
server will create its own SparkContext, and return a job ID for subsequent querying:

    curl -d "input.string = a b c a b see" 'localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample'
    {
      "status": "STARTED",
      "result": {
        "jobId": "5453779a-f004-45fc-a11d-a39dae0f9bf4",
        "context": "b7ea0eb5-spark.jobserver.WordCountExample"
      }
    }⏎

NOTE: If you want to feed in a text file config and POST using curl, you want the `--data-binary` option, otherwise
curl will munge your line separator chars.  Like:

    curl --data-binary @my-job-config.json 'localhost:8090/jobs?appNam=...'

From this point, you could asynchronously query the status and results:

    curl localhost:8090/jobs/5453779a-f004-45fc-a11d-a39dae0f9bf4
    {
      "status": "OK",
      "result": {
        "a": 2,
        "b": 2,
        "c": 1,
        "see": 1
      }
    }⏎

Note that you could append `&sync=true` when you POST to /jobs to get the results back in one request, but for
real clusters and most jobs this may be too slow.

Another way of running this job is in a pre-created context.  Start a new context:

    curl -d "" 'localhost:8090/contexts/test-context?num-cpu-cores=4&mem-per-node=512m'
    OK⏎

You can verify that the context has been created:

    curl localhost:8090/contexts
    ["test-context"]⏎

Now let's run the job in the context and get the results back right away:

    curl -d "input.string = a b c a b see" 'localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample&context=test-context&sync=true'
    {
      "status": "OK",
      "result": {
        "a": 2,
        "b": 2,
        "c": 1,
        "see": 1
      }
    }⏎

Note the addition of `context=` and `sync=true`.

## Create a Job Server Project
In your `build.sbt`, add this to use the job server jar:

	resolvers += "Ooyala Bintray" at "http://dl.bintray.com/ooyala/maven"

	libraryDependencies += "ooyala.cnd" % "job-server" % "0.3.1" % "provided"                                                                                                                                                                                                             

For most use cases it's better to have the dependencies be "provided" because you don't want SBT assembly to include the whole job server jar.

## Deployment

1. Copy `config/local.sh.template` to `<environment>.sh` and edit as appropriate.
2. `bin/server_deploy.sh <environment>` -- this packages the job server along with config files and pushes
   it to the remotes you have configured in `<environment>.sh`
3. On the remote server, start it in the deployed directory with `server_start.sh`.

Note: to test out the deploy to a local staging dir, or package the job server for Mesos,
use `bin/server_package.sh <environment>`.

## Architecture

The job server is intended to be run as one or more independent processes, separate from the Spark cluster
(though it very well may be colocated with say the Master).

At first glance, it seems many of these functions (eg job management) could be integrated into the Spark standalone master.  While this is true, we believe there are many significant reasons to keep it separate:

- We want the job server to work for Mesos and YARN as well
- Spark and Mesos masters are organized around "applications" or contexts, but the job server supports running many discrete "jobs" inside a single context
- We want it to support Shark functionality in the future
- Loose coupling allows for flexible HA arrangements (multiple job servers targeting same standalone master, or possibly multiple Spark clusters per job server)

Flow diagrams are checked in in the doc/ subdirectory.  .diagram files are for websequencediagrams.com... check them out, they really will help you understand the flow of messages between actors.

## API

### Jars

    GET /jars            - lists all the jars and the last upload timestamp
    POST /jars/<appName> - uploads a new jar under <appName>

### Contexts

    GET /contexts         - lists all current contexts
    POST /contexts/<name> - creates a new context
    DELETE /contexts/<name> - stops a context and all jobs running in it

### Jobs

Jobs submitted to the job server must implement a `SparkJob` trait.  It has a main `runJob` method which is
passed a SparkContext and a typesafe Config object.  Results returned by the method are made available through
the REST API.

    GET /jobs                - Lists the last N jobs
    POST /jobs               - Starts a new job, use ?sync=true to wait for results
    GET /jobs/<jobId>        - Gets the result or status of a specific job
    GET /jobs/<jobId>/config - Gets the job configuration

### Context configuration

A number of context-specific settings can be controlled when creating a context (POST /contexts) or running an
ad-hoc job (which creates a context on the spot).

When creating a context via POST /contexts, the query params are used to override the default configuration in
spark.context-settings.  For example,

    POST /contexts/my-new-context?num-cpu-cores=10

would override the default spark.context-settings.num-cpu-cores setting.

When starting a job, and the context= query param is not specified, then an ad-hoc context is created.  Any
settings specified in spark.context-settings will override the defaults in the job server config when it is
started up.

Any spark configuration param can be overridden either in POST /contexts query params, or through `spark
.context-settings` job configuration.  In addition, `num-cpu-cores` maps to `spark.cores.max`, and `mem-per-
node` maps to `spark.executor.memory`.  Therefore the following are all equivalent:

    POST /contexts/my-new-context?num-cpu-cores=10

    POST /contexts/my-new-context?spark.cores.max=10

or in the job config when using POST /jobs,

    spark.context-settings {
        spark.cores.max = 10
    }

For the exact context configuration parameters, see JobManagerActor docs as well as application.conf.

### Job Result Serialization

The result returned by the `SparkJob` `runJob` method is serialized by the job server into JSON for routes
that return the result (GET /jobs with sync=true, GET /jobs/<jobId>).  Currently the following types can be
serialized properly:

- String, Int, Long, Double, Float, Boolean
- Scala Map's with string key values (non-string keys may be converted to strings)
- Scala Seq's
- Array's
- Anything that implements Product (Option, case classes) -- they will be serialized as lists
- Maps and Seqs may contain nested values of any of the above

If we encounter a data type that is not supported, then the entire result will be serialized to a string.

## Contribution and Development
Contributions via Github Pull Request are welcome.  See the TODO for some ideas.

- From the "master" project, please run "test" to ensure nothing is broken.
   - You may need to set `SPARK_LOCAL_IP` to `localhost` to ensure Akka port can bind successfully
- Logging for tests goes to "job-server-test.log"
- Run `scoverage:test` to check the code coverage and improve it
- Please run scalastyle to ensure your code changes don't break the style guide
- Do "re-start" from SBT for quick restarts of the job server process
- Please update the g8 template if you change the SparkJob API

### Publishing packages

- Be sure you are in the master project
- Run `test` to ensure all tests pass
- Now just run `publish` and package will be published to bintray
  
## License
Apache 2.0, see LICENSE.md

Copyright(c) 2014, Ooyala, Inc.

## TODO

- Add Swagger support.  See the spray-swagger project.
- Implement a main index.html.  It should display:
    - All the running contexts
    - All the current jobs in each running context

- Implement an interactive SQL window.  See: [spark-admin](https://github.com/adatao/spark-admin)

- Use `SparkContext.setJobGroup` with the job ID
- Support job cancellation via `cancelJobGroup`
- Stream the current job progress via a Listener
- Add routes to return stage info for a job.  Persist it via DAO so that we can always retrieve stage / performance info
  even for historical jobs.  This would be pretty kickass.
