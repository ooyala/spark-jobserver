package spark.jobserver

import akka.actor.{ActorRef, Props, PoisonPill}
import com.typesafe.config.Config
import java.net.URL
import java.util.concurrent.atomic.AtomicInteger
import ooyala.common.akka.InstrumentedActor
import org.apache.spark.{ SparkEnv, SparkContext }
import org.joda.time.DateTime
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import spark.jobserver.ContextSupervisor.StopContext
import spark.jobserver.io.{ JobDAO, JobInfo, JarInfo }
import spark.jobserver.util.{ContextURLClassLoader, SparkJobUtils}

import spark.jobserver.api.{SparkJobValid, SparkJobInvalid, NamedRddSupport, NamedRdds} 

object JobManagerActor {
  // Messages
  case object Initialize
  case class StartJob(appName: String, classPath: String, config: Config,
                      subscribedEvents: Set[Class[_]])

  // Results/Data
  case class Initialized(resultActor: ActorRef)
  case class InitError(t: Throwable)
  case class JobLoadingError(err: Throwable)

  // Akka 2.2.x style actor props for actor creation
  def props(dao: JobDAO, name: String, master: String, config: Config, isAdHoc: Boolean,
            resultActorRef: Option[ActorRef] = None) =
    Props(classOf[JobManagerActor], dao, name, master, config, isAdHoc, resultActorRef)
}

/**
 * The JobManager actor supervises jobs running in a single SparkContext, as well as shared metadata.
 * It creates a SparkContext.
 * It also creates and supervises a JobResultActor and JobStatusActor, although an existing JobResultActor
 * can be passed in as well.
 *
 * == Configuration ==
 * {{{
 *  num-cpu-cores = 4         # Total # of CPU cores to allocate across the cluster
 *  memory-per-node = 512m    # -Xmx style memory string for total memory to use for executor on one node
 *  dependent-jar-uris = ["local://opt/foo/my-foo-lib.jar"]   # URIs for dependent jars to load for entire context
 *  max-jobs-per-context = 4  # Max # of jobs to run at the same time
 *  spark.mesos.coarse = true  # per-context, rather than per-job, resource allocation
 *  rdd-ttl = 24 h            # time-to-live for RDDs in a SparkContext.  Don't specify = forever
 * }}}
 */
class JobManagerActor(dao: JobDAO,
                      contextName: String,
                      sparkMaster: String,
                      contextConfig: Config,
                      isAdHoc: Boolean,
                      resultActorRef: Option[ActorRef] = None) extends InstrumentedActor {

  import CommonMessages._
  import JobManagerActor._
  import scala.util.control.Breaks._
  import collection.JavaConverters._
  import context.dispatcher       // for futures to work

  val config = context.system.settings.config

  var sparkContext: SparkContext = _
  var sparkEnv: SparkEnv = _
  protected var rddManagerActor: ActorRef = _

  private val maxRunningJobs = {
    val cpuCores = Runtime.getRuntime.availableProcessors
    Try(contextConfig.getInt("spark.jobserver.max-jobs-per-context")).getOrElse(cpuCores)
  }
  private val currentRunningJobs = new AtomicInteger(0)

  // When the job cache retrieves a jar from the DAO, it also adds it to the SparkContext for distribution
  // to executors.  We do not want to add the same jar every time we start a new job, as that will cause
  // the executors to re-download the jar every time, and causes race conditions.
  // NOTE: It's important that jobCache be lazy as sparkContext is not initialized until later
  private val jobCacheSize = Try(config.getInt("spark.job-cache.max-entries")).getOrElse(10000)
  // Use Spark Context's built in classloader when SPARK-1230 is merged.
  private val jarLoader = new ContextURLClassLoader(Array[URL](), getClass.getClassLoader)
  lazy val jobCache = new JobCache(jobCacheSize, dao, sparkContext, jarLoader)

  private val statusActor = context.actorOf(Props(classOf[JobStatusActor], dao), "status-actor")
  protected val resultActor = resultActorRef.getOrElse(context.actorOf(Props[JobResultActor], "result-actor"))

  override def postStop() {
    logger.info("Shutting down SparkContext {}", contextName)
    Option(sparkContext).foreach(_.stop())
  }

  def wrappedReceive: Receive = {
    case Initialize =>
      try {
        sparkContext = createContextFromConfig()
        sparkEnv = SparkEnv.get
        rddManagerActor = context.actorOf(Props(classOf[RddManagerActor], sparkContext), "rdd-manager-actor")
        getSideJars(contextConfig).foreach { jarPath => sparkContext.addJar(jarPath) }
        sender ! Initialized(resultActor)
      } catch {
        case t: Throwable =>
          logger.error("Failed to create context " + contextName + ", shutting down actor", t)
          sender ! InitError(t)
          self ! PoisonPill
      }

    case StartJob(appName, classPath, jobConfig, events) =>
      startJobInternal(appName, classPath, jobConfig, events, sparkContext, sparkEnv, rddManagerActor)
  }

  def startJobInternal(appName: String,
                       classPath: String,
                       jobConfig: Config,
                       events: Set[Class[_]],
                       sparkContext: SparkContext,
                       sparkEnv: SparkEnv,
                       rddManagerActor: ActorRef): Option[Future[Any]] = {
    var future: Option[Future[Any]] = None
    breakable {
      val lastUploadTime = dao.getLastUploadTime(appName)
      if (!lastUploadTime.isDefined) {
        sender ! NoSuchApplication
        break
      }

      // Check appName, classPath from jar
      val jarInfo = JarInfo(appName, lastUploadTime.get)
      val jobId = java.util.UUID.randomUUID().toString()
      logger.info("Loading class {} for app {}", classPath, appName: Any)
      val jobJarInfo = try {
        jobCache.getSparkJob(jarInfo.appName, jarInfo.uploadTime, classPath)
      } catch {
        case _: ClassNotFoundException =>
          sender ! NoSuchClass
          postEachJob()
          break
          null // needed for inferring type of return value
        case err: Throwable =>
          sender ! JobLoadingError(err)
          postEachJob()
          break
          null
      }

      // Automatically subscribe the sender to events so it starts getting them right away
      resultActor ! Subscribe(jobId, sender, events)
      statusActor ! Subscribe(jobId, sender, events)

      val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, DateTime.now(), None, None)
      future =
        Option(getJobFuture(jobJarInfo, jobInfo, jobConfig, sender, sparkContext, sparkEnv,
                            rddManagerActor))
    }

    future
  }

  private def getJobFuture(jobJarInfo: JobJarInfo,
                           jobInfo: JobInfo,
                           jobConfig: Config,
                           subscriber: ActorRef,
                           sparkContext: SparkContext,
                           sparkEnv: SparkEnv,
                           rddManagerActor: ActorRef): Future[Any] = {
    // Use the SparkContext's ActorSystem threadpool for the futures, so we don't corrupt our own
    implicit val executionContext = sparkEnv.actorSystem

    val jobId = jobInfo.jobId
    val constructor = jobJarInfo.constructor
    logger.info("Starting Spark job {} [{}]...", jobId: Any, jobJarInfo.className)

    // Atomically increment the number of currently running jobs. If the old value already exceeded the
    // limit, decrement it back, send an error message to the sender, and return a dummy future with
    // nothing in it.
    if (currentRunningJobs.getAndIncrement() >= maxRunningJobs) {
      currentRunningJobs.decrementAndGet()
      sender ! NoJobSlotsAvailable(maxRunningJobs)
      return Future[Any](None)
    }

    Future {
      org.slf4j.MDC.put("jobId", jobId)
      logger.info("Starting job future thread")

      // Need to re-set the SparkEnv because it's thread-local and the Future runs on a diff thread
      SparkEnv.set(sparkEnv)

      // Use the Spark driver's class loader as it knows about all our jars already
      // NOTE: This may not even be necessary if we set the driver ActorSystem classloader correctly
      Thread.currentThread.setContextClassLoader(jarLoader)
      val job = constructor()
      if (job.isInstanceOf[NamedRddSupport]) {
        val namedRdds = job.asInstanceOf[NamedRddSupport].namedRddsPrivate
        if (namedRdds.get() == null) {
          namedRdds.compareAndSet(null, new JobServerNamedRdds(rddManagerActor))
        }
      }

      try {
        statusActor ! JobStatusActor.JobInit(jobInfo)

        job.validate(sparkContext, jobConfig) match {
          case SparkJobInvalid(reason) => {
            val err = new Throwable(reason)
            statusActor ! JobValidationFailed(jobId, DateTime.now(), err)
            throw err
          }
          case SparkJobValid => {
            statusActor ! JobStarted(jobId: String, contextName, jobInfo.startTime)
            job.runJob(sparkContext, jobConfig)
          }
        }
      } finally {
        org.slf4j.MDC.remove("jobId")
      }
    }.andThen {
      case Success(result: Any) =>
        statusActor ! JobFinished(jobId, DateTime.now())
        resultActor ! JobResult(jobId, result)
      case Failure(error: Throwable) =>
        // If and only if job validation fails, JobErroredOut message is dropped silently in JobStatusActor.
        statusActor ! JobErroredOut(jobId, DateTime.now(), error)
        logger.warn("Exception from job " + jobId + ": ", error)
    }.andThen {
      case _ =>
        // Make sure to decrement the count of running jobs when a job finishes, in both success and failure
        // cases.
        resultActor ! Unsubscribe(jobId, subscriber)
        statusActor ! Unsubscribe(jobId, subscriber)
        currentRunningJobs.getAndDecrement()
        postEachJob()
    }
  }

  def createContextFromConfig(contextName: String = contextName): SparkContext = {
    val conf = SparkJobUtils.configToSparkConf(config, contextConfig, sparkMaster, contextName)

    // Set number of akka threads
    // TODO: need to figure out how many extra threads spark needs, besides the job threads
    conf.set("spark.akka.threads", (maxRunningJobs + 4).toString)

    new SparkContext(conf)
  }

  // This method should be called after each job is succeeded or failed
  private def postEachJob() {
    // Delete the JobManagerActor after each adhoc job
    if (isAdHoc)
      context.parent ! StopContext(contextName) // its parent is LocalContextSupervisorActor
  }

  // "Side jars" are jars besides the main job jar that are needed for running the job.
  // They are loaded from the context/job config.
  // Each one should be an URL (http, ftp, hdfs, local, or file). local URLs are local files
  // present on every node, whereas file:// will be assumed only present on driver node
  private def getSideJars(config: Config): Seq[String] =
    Try(config.getStringList("dependent-jar-uris").asScala.toSeq).getOrElse(Nil)
}
