package ooyala.common.akka.metrics

sealed abstract class MetricsLevel(
    val level: Int,
    val name: String)
  extends Ordered[MetricsLevel] {

  def compare(that: MetricsLevel) = level - that.level

  override def toString = name
}

/**
 * Specifies the different levels of details on metrics.
 */
object MetricsLevel {
  case object NONE extends MetricsLevel(0, "NONE")
  case object BASIC extends MetricsLevel(1, "BASIC")
  case object FINE extends MetricsLevel(2, "FINE")
  case object FINER extends MetricsLevel(3, "FINER")

  /**
   * Converts an integer value to a MetricsLevel
   *
   * @param level an integer value
   * @return a MetricsLevel for the integer value
   */
  def valueOf(level: Int): MetricsLevel = level match {
    case 0 => NONE
    case 1 => BASIC
    case 2 => FINE
    case 3 => FINER
    case _ => {
      throw new IllegalArgumentException("Metrics level must be 0 to 3")
    }
  }
}