package spark.jobserver.io

import java.io.File

import com.google.common.io.Files
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfter, FunSpec}
import spark.jobserver.TestJarFinder

class JobCacheDAOSpec extends TestJarFinder with FunSpec with ShouldMatchers with BeforeAndAfter {
  private val config = ConfigFactory.load("local.test.jobcachedao.conf")

  var dao: JobCacheDAO = _

  // *** TEST DATA ***
  val time: DateTime = new DateTime()
  val throwable: Throwable = new Throwable("test-error")
  // jar test data
  val jarInfo: JarInfo = genJarInfo(false, false)
  val jarBytes: Array[Byte] = Files.toByteArray(testJar)
  var jarFile: File = new File(config.getString("spark.jobserver.cachedao.rootdir"),
                               createJarName(jarInfo.appName, jarInfo.uploadTime) + ".jar")

  // jobInfo test data
  val jobInfoNoEndNoErr:JobInfo = genJobInfo(jarInfo, false, false, false)
  val expectedJobInfo = jobInfoNoEndNoErr
  val jobInfoSomeEndNoErr: JobInfo = genJobInfo(jarInfo, true, false, false)
  val jobInfoNoEndSomeErr: JobInfo = genJobInfo(jarInfo, false, true, false)
  val jobInfoSomeEndSomeErr: JobInfo = genJobInfo(jarInfo, true, true, false)

  // job config test data
  val jobId: String = jobInfoNoEndNoErr.jobId
  val jobConfig: Config = ConfigFactory.parseString("{marco=pollo}")
  val expectedConfig: Config = ConfigFactory.empty().withValue("marco", ConfigValueFactory.fromAnyRef("pollo"))

  private def createJarName(appName: String, uploadTime: DateTime): String = appName + "-" + uploadTime.toString().replace(':', '_')

  // Helper functions and closures!!
  private def genJarInfoClosure = {
    var appCount: Int = 0
    var timeCount: Int = 0

    def genTestJarInfo(newAppName: Boolean, newTime: Boolean): JarInfo = {
      appCount = appCount + (if (newAppName) 1 else 0)
      timeCount = timeCount + (if (newTime) 1 else 0)

      val app = "test-appName" + appCount
      val upload = if (newTime) time.plusMinutes(timeCount) else time

      JarInfo(app, upload)
    }

    genTestJarInfo _
  }

  private def genJobInfoClosure = {
    var count: Int = 0

    def genTestJobInfo(jarInfo: JarInfo, hasEndTime: Boolean, hasError: Boolean, isNew:Boolean): JobInfo = {
      count = count + (if (isNew) 1 else 0)

      val id: String = "test-id" + count
      val contextName: String = "test-context"
      val classPath: String = "test-classpath"
      val startTime: DateTime = time

      val noEndTime: Option[DateTime] = None
      val someEndTime: Option[DateTime] = Some(time) // Any DateTime Option is fine
      val noError: Option[Throwable] = None
      val someError: Option[Throwable] = Some(throwable)

      val endTime: Option[DateTime] = if (hasEndTime) someEndTime else noEndTime
      val error: Option[Throwable] = if (hasError) someError else noError

      JobInfo(id, contextName, jarInfo, classPath, startTime, endTime, error)
    }

    genTestJobInfo _
  }

  def genJarInfo = genJarInfoClosure
  def genJobInfo = genJobInfoClosure
  //**********************************

  before {
    FileUtils.deleteDirectory(new File(config.getString("spark.jobserver.cachedao.rootdir")))
    dao = new JobCacheDAO(config)
    jarFile.delete()
  }

  describe("save and get the jars") {
    it("should be able to save one jar and get it back") {
      // check the pre-condition
      jarFile.exists() should equal (false)

      // save
      dao.saveJar(jarInfo.appName, jarInfo.uploadTime, jarBytes)

      // read it back
      val apps = dao.getApps

      // test
      jarFile.exists() should equal (true)
      apps.keySet should equal (Set(jarInfo.appName))
      apps(jarInfo.appName) should equal (jarInfo.uploadTime)
    }
  }

  describe("saveJobConfig() and getJobConfigs() tests") {
    it("should provide an empty map on getJobConfigs() for an empty CONFIGS table") {
      (Map.empty[String, Config]) should equal (dao.getJobConfigs)
    }

    it("should save and get the same config") {
      // save job config
      dao.saveJobConfig(jobId, jobConfig)

      // get all configs
      val configs = dao.getJobConfigs

      // test
      configs.keySet should equal (Set(jobId))
      configs(jobId) should equal (expectedConfig)
    }

    it("should save and get the large config") {
      val total = 5000
      val str = "{" + (1 to total).map(i => s"key-$i=value-$i").mkString(",") + "}"

      str.getBytes.length > 65535 should equal (true)

      val jobConfig: Config = ConfigFactory.parseString(str)
      // save job config
      dao.saveJobConfig(jobId, jobConfig)

      // get all configs
      val configs = dao.getJobConfigs

      // test
      configs.keySet should equal (Set(jobId))
      configs(jobId).entrySet().size() should equal (total)
    }
  }

  describe("Basic saveJobInfo() and getJobInfos() tests") {
    it("should provide an empty map on getJobInfos() for an empty JOBS table") {
      (Map.empty[String, JobInfo]) should equal (dao.getJobInfos)
    }

    it("should save a new JobInfo and get the same JobInfo") {
      // save JobInfo
      dao.saveJobInfo(jobInfoNoEndNoErr)

      // get all JobInfos
      val jobs = dao.getJobInfos

      // test
      jobs.keySet should equal (Set(jobId))
      jobs(jobId) should equal (expectedJobInfo)
    }
  }
}
