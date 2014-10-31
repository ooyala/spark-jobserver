package ooyala.common.akka.metrics

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

class MetricsLevelSpec extends FunSpec with ShouldMatchers {

  describe("MetricsLevel") {
    it("should return a valid metric level") {
      MetricsLevel.valueOf(0) should equal(MetricsLevel.NONE)

      MetricsLevel.valueOf(1) should equal(MetricsLevel.BASIC)

      MetricsLevel.valueOf(2) should equal(MetricsLevel.FINE)

      MetricsLevel.valueOf(3) should equal(MetricsLevel.FINER)
    }

    it("should throw exceptions") {
      evaluating {
        MetricsLevel.valueOf(-1)
      } should produce [IllegalArgumentException]

      evaluating {
        MetricsLevel.valueOf(4)
      } should produce [IllegalArgumentException]
    }
  }
}
