package spark.jobserver.io

import java.io._

import com.typesafe.config._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.util.LRUCache

import scala.collection.mutable
import scala.util.Try

/**
 * Store job information (JobInfo, config) in memory cache only, but jar files still in file system.
 *
 *
 * @param config
 */
class JobCacheDAO(config: Config) extends JobDAO {
  private val logger = LoggerFactory.getLogger(getClass)

  private val charsetName = "UTF-8"

  // appName to its set of upload times. Decreasing times in the seq.
  private val apps = mutable.HashMap.empty[String, Seq[DateTime]]
  // jobId to its (JobInfo, config)
  private val jobs = new LRUCache[String, (JobInfo, Config)](this.getClass,
    Try(config.getInt("spark.jobserver.cachedao.size")).getOrElse(500))

  private val rootDir = getOrElse(config.getString("spark.jobserver.cachedao.rootdir"),
    "/tmp/spark-jobserver/cachedao/data")
  private val rootDirFile = new File(rootDir)
  logger.info("rootDir is " + rootDirFile.getAbsolutePath)

  private val jarsFile = new File(rootDirFile, "jars.data")
  private var jarsOutputStream: DataOutputStream = null

  init()

  private def init() {
    // create the date directory if it doesn't exist
    if (!rootDirFile.exists()) {
      if (!rootDirFile.mkdirs()) {
        throw new RuntimeException("Could not create directory " + rootDir)
      }
    }

    // read back all apps info during startup
    if (jarsFile.exists()) {
      val in = new DataInputStream(new BufferedInputStream(new FileInputStream(jarsFile)))
      try {
        while (true) {
          val jarInfo = readJarInfo(in)
          addJar(jarInfo.appName, jarInfo.uploadTime)
        }
      } catch {
        case e: EOFException => // do nothing

      } finally {
        in.close()
      }
    }

    // Don't buffer the stream. I want the apps meta data log directly into the file.
    // Otherwise, server crash will lose the buffer data.
    jarsOutputStream = new DataOutputStream(new FileOutputStream(jarsFile, true))
  }

  override def saveJar(appName: String, uploadTime: DateTime, jarBytes: Array[Byte]) {
    // The order is important. Save the jar file first and then log it into jobsFile.
    val outFile = new File(rootDir, createJarName(appName, uploadTime) + ".jar")
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      logger.debug("Writing {} bytes to file {}", jarBytes.size, outFile.getPath)
      bos.write(jarBytes)
      bos.flush()
    } finally {
      bos.close()
    }

    // log it into jobsFile
    writeJarInfo(jarsOutputStream, JarInfo(appName, uploadTime))

    // track the new jar in memory
    addJar(appName, uploadTime)
  }

  private def writeJarInfo(out: DataOutputStream, jarInfo: JarInfo) {
    out.writeUTF(jarInfo.appName)
    out.writeLong(jarInfo.uploadTime.getMillis)
  }

  private def readJarInfo(in: DataInputStream) = JarInfo(in.readUTF, new DateTime(in.readLong))

  private def addJar(appName: String, uploadTime: DateTime) {
    if (apps.contains(appName)) {
      apps(appName) = uploadTime +: apps(appName) // latest time comes first
    } else {
      apps(appName) = Seq(uploadTime)
    }
  }

  def getApps: Map[String, DateTime] = apps.map {
    case (appName, uploadTimes) =>
      appName -> uploadTimes.head
    }.toMap

  override def retrieveJarFile(appName: String, uploadTime: DateTime): String =
    new File(rootDir, createJarName(appName, uploadTime) + ".jar").getAbsolutePath

  private def createJarName(appName: String, uploadTime: DateTime): String =
    appName + "-" + uploadTime.toString().replace(':', '_')

  override def saveJobInfo(jobInfo: JobInfo) {
    jobs.synchronized {
      jobs.get(jobInfo.jobId) match {
        case Some((_, config)) => jobs.put(jobInfo.jobId, (jobInfo, config))
        case None => jobs.put(jobInfo.jobId, (jobInfo, null))
      }
    }
  }

  private def readError(in: DataInputStream) = {
    val error = in.readUTF()
    if (error == "") None else Some(new Throwable(error))
  }

  override def getJobInfos: Map[String, JobInfo] = jobs.toMap.map {case (id, (jobInfo, _)) => (id, jobInfo)}

  override def getJobInfosLimit(limit: Int): Map[String, JobInfo] = getJobInfos.takeRight(limit)

  override def getJobInfo(jobId: String): Option[JobInfo] =  jobs.get(jobId).map(_._1)

  override def saveJobConfig(jobId: String, config: Config) {
    jobs.synchronized {
      jobs.get(jobId) match {
        case Some((jobInfo, _)) => jobs.put(jobId, (jobInfo, config))
        case None => jobs.put(jobId, (null, config))
      }
    }
  }

  override def getJobConfigs: Map[String, Config] = jobs.toMap.map {case (id, (_, config)) => (id, config)}

}
