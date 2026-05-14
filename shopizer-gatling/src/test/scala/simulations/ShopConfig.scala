package simulations

import io.gatling.core.Predef._
import io.gatling.core.controller.inject.open.OpenInjectionStep
import io.gatling.http.Predef._
import scala.concurrent.duration._

/**
 * Shared configuration read from system properties.
 * Every property has a sensible default so tests work out of the box
 * against a locally running Shopizer instance.
 *
 * Override at runtime, e.g.:
 *   mvn gatling:test -DbaseUrl=http://staging.example.com:8080 -DbrowseUsers=50
 */
object ShopConfig {

  // Target application
  val baseUrl: String    = sys.props.getOrElse("baseUrl",    "http://localhost:8081")
  val storeCode: String  = sys.props.getOrElse("storeCode",  "DEFAULT")
  val lang: String       = sys.props.getOrElse("lang",       "en")

  // Load shape (all values are integers representing users / seconds)
  val browseUsers: Int   = sys.props.getOrElse("browseUsers",   "20").toInt
  val checkoutUsers: Int = sys.props.getOrElse("checkoutUsers",  "5").toInt
  val rampSeconds: Int   = sys.props.getOrElse("rampSeconds",   "30").toInt
  val holdSeconds: Int   = sys.props.getOrElse("holdSeconds",  "120").toInt

  // Diurnal (normal-distribution) load shape.
  // The "day" is compressed into `dayDurationSeconds`; load peaks at its middle
  // with standard deviation `stdDevFraction` × dayDuration.
  val dayDurationSeconds: Int = sys.props.getOrElse("dayDurationSeconds", "1800").toInt
  val peakUsersPerSec: Double = sys.props.getOrElse("peakUsersPerSec",    "5.0").toDouble
  val stdDevFraction: Double  = sys.props.getOrElse("stdDevFraction",     "0.15").toDouble
  val diurnalSteps: Int       = sys.props.getOrElse("diurnalSteps",       "30").toInt

  /**
   * Builds a sequence of `constantUsersPerSec` steps that together approximate
   * a normal distribution over `totalSeconds`, peaking at `peak` users/sec at
   * the midpoint with standard deviation `sigma` (seconds).
   *
   * Each step lasts `totalSeconds / steps` seconds and uses the Gaussian PDF
   * value at that step's midpoint, rescaled so the peak equals `peak`.
   */
  def gaussianInjection(
    totalSeconds: Int  = dayDurationSeconds,
    peak: Double       = peakUsersPerSec,
    sigma: Double      = dayDurationSeconds * stdDevFraction,
    steps: Int         = diurnalSteps
  ): Seq[OpenInjectionStep] = {
    val mid     = totalSeconds / 2.0
    val step    = totalSeconds.toDouble / steps
    val twoSig2 = 2.0 * sigma * sigma
    (0 until steps).map { i =>
      val t    = (i + 0.5) * step
      val rate = peak * math.exp(-math.pow(t - mid, 2) / twoSig2)
      constantUsersPerSec(math.max(rate, 0.01)) during step.toInt.seconds
    }
  }

  /** Same shape scaled by `factor` — used for the 10% purchase flow. */
  def gaussianInjectionScaled(factor: Double): Seq[OpenInjectionStep] =
    gaussianInjection(peak = peakUsersPerSec * factor)

  // Base HTTP protocol – shared by all simulations
  val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")
    .acceptEncodingHeader("gzip, deflate")
    .userAgentHeader("Gatling/ShopLoadTest/1.0")
    .disableWarmUp
}
