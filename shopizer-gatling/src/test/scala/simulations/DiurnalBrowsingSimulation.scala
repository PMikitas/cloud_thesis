package simulations

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._

/**
 * Diurnal catalog-discovery simulation.
 *
 * Models an anonymous user who lands on the storefront, wanders through
 * categories and products, and randomly goes "back" — returning to the
 * category list or a sibling category before drilling in again.
 *
 * Load follows a normal distribution over the configured day duration,
 * peaking at `peakUsersPerSec` at the midpoint (± stdDevFraction × day).
 *
 * Tunable via system properties (see ShopConfig):
 *   -DdayDurationSeconds=1800   total simulation length (compressed "day")
 *   -DpeakUsersPerSec=5.0       arrivals/sec at peak
 *   -DstdDevFraction=0.15       spread (±3σ ≈ ±0.45 × day)
 *   -DdiurnalSteps=30           how many constant-rate slices to build the curve
 *
 * Run:
 *   mvn gatling:test -Dgatling.simulationClass=simulations.DiurnalBrowsingSimulation
 */
class DiurnalBrowsingSimulation extends Simulation {

  import ShopConfig._

  before {
    PreChecks.assertReady(baseUrl, skipSearch = true)
  }

  // ── Requests ──────────────────────────────────────────────────────────────
  private val getCategories = http("GET /category")
    .get("/api/v1/category")
    .queryParam("lang", lang)
    .queryParam("store", storeCode)
    .check(status.is(200))
    .check(jsonPath("$.categories[*].id").findAll.saveAs("categoryIds"))

  private val pickCategory = exec { session =>
    val ids = session("categoryIds").as[Seq[String]]
    if (ids.isEmpty) session else session.set("categoryId", ids(util.Random.nextInt(ids.size)))
  }

  private val listProductsByCategory = http("GET /products?category")
    .get("/api/v1/products")
    .queryParam("category", "#{categoryId}")
    .queryParam("page", "#{productPage}")
    .queryParam("count", "20")
    .queryParam("lang", lang)
    .queryParam("store", storeCode)
    .check(status.is(200))
    .check(jsonPath("$.products[*].id").findAll.optional.saveAs("productIds"))

  private val pickProduct = exec { session =>
    val ids = session("productIds").asOption[Seq[String]].getOrElse(Seq.empty)
    if (ids.isEmpty) session else session.set("productId", ids(util.Random.nextInt(ids.size)))
  }

  // ── Flow with random back-navigation ──────────────────────────────────────
  // On each category visit the user pages through products, possibly views a
  // product, then either:
  //   - goes back to the category list (~40%)
  //   - picks another category (~40%)
  //   - ends the session (~20%)
  //
  // `productPage` is advanced or reset as the user "pages" or "goes back".
  private val discoveryFlow = scenario("Diurnal Browsing")
    .exec(initTracingHeaders)
    .exec(_.set("productPage", 0))
    .exec(getCategories)
    .pause(1, 3)
    .asLongAs(session => util.Random.nextDouble() < 0.8, exitASAP = false) {
      exec(pickCategory)
        .exec(listProductsByCategory)
        .pause(1, 4)
        .doIf(session => session.contains("productIds")) {
          exec(pickProduct).pause(2, 6)
        }
        .randomSwitch(
          40d -> exec { s => s.set("productPage", s("productPage").as[Int] + 1) }
                   .exec(listProductsByCategory).pause(1, 3),
          40d -> exec(_.set("productPage", 0)).exec(getCategories).pause(1, 2),
          20d -> exec(s => s)
        )
    }

  // ── Load profile: normal distribution over the day ────────────────────────
  setUp(
    discoveryFlow.inject(gaussianInjection())
  )
    .protocols(httpProtocol)
    .assertions(
      global.successfulRequests.percent.gte(95),
      global.responseTime.percentile3.lte(2000)
    )
}
