package spark.jobserver

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import scala.concurrent.duration._
import spark.jobserver.io.JobSqlDAO


class LocalContextSupervisorHaSpec(system: ActorSystem) extends TestKit(system) with ImplicitSender
    with FunSpec with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {

  def this() = this(ActorSystem("test", LocalContextSupervisorSpec.config))

  override def afterAll() {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(system)
  }

  // Expects the response to GetContext message for the named context.
  private def expectResponseToGetContext(contextName: String) = {
    expectMsgPF() {
      case (manager: ActorRef, resultActor: ActorRef) =>
        manager.path.name should equal(contextName)
        resultActor.path.name should equal("result-actor")
    }
  }

  var supervisor_a: ActorRef = _
  var supervisor_b: ActorRef = _
  val contextConfig = LocalContextSupervisorSpec.config.getConfig("spark.context-settings")

  implicit val timeout: Timeout = 10 seconds

  // This is needed to help tests pass on some MBPs when working from home
  System.setProperty("spark.driver.host", "localhost")

  before {
    val sqlDaoConfigStr = "spark.jobserver.sqldao.h2.url=\"jdbc:h2:file:" +
      "/tmp/spark-job-server-test/sqldao/data/" + java.util.UUID.randomUUID() + "\""
    val sqlDaoConfig = ConfigFactory.parseString(sqlDaoConfigStr)

    val dao_a = new JobSqlDAO(sqlDaoConfig)
    supervisor_a = system.actorOf(Props(classOf[LocalContextSupervisorActor], dao_a))

    val dao_b = new JobSqlDAO(sqlDaoConfig)
    supervisor_b = system.actorOf(Props(classOf[LocalContextSupervisorActor], dao_b))
  }

  after {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(supervisor_a)
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(supervisor_b)
  }

  import ContextSupervisor._

  describe("context high availability") {
    it("should return no such context if DAO hasn't added it") {
      supervisor_a ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)

      supervisor_b ! GetContext("c2")
      expectMsg(NoSuchContext)
    }

    it("should allow two context supervisors to share the same context config") {
      supervisor_a ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)

      supervisor_b ! GetContext("c1")
      expectResponseToGetContext("c1")
    }

    it("should return ContextJobDaoError if context config already added by DAO") {
      supervisor_a ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)

      supervisor_b ! AddContext("c1", contextConfig)
      expectMsgType[ContextJobDaoError]
    }

    it("should retrieve context config from persistent storage after context stopped") {
      supervisor_a ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)

      supervisor_b ! GetContext("c1")
      expectResponseToGetContext("c1")

      supervisor_b ! StopContext("c1")
      expectMsg(ContextStopped)

      Thread.sleep(2000) // wait for a while since deleting context is an asyc call

      supervisor_b ! GetContext("c1")
      expectResponseToGetContext("c1")
    }
  }
}