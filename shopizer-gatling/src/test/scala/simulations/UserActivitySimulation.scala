package simulations

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration._

/**
 * Simulates realistic shopper activity against the Shopizer storefront.
 *
 * Per virtual user a session runs for `sessionDurationSeconds` during which an
 * action stack is repeated in a loop:
 *   1. Browse catalog: list categories → pick a random category → list products.
 *   2. With probability `addToCartPct` (default 0.40): add 1–5 random items to
 *      the cart. A newcomer registers & logs in lazily before its first add.
 *   3. With probability `checkoutPct` (default 0.85) given add-to-cart: review
 *      cart and place a FREE-payment order.
 *
 * User mix (tunable via system properties):
 *   - 70 % registered users: register & log in at session start, then loop.
 *   - 30 % newcomers: browse anonymously and only register when a cart action
 *     is triggered.
 *
 * Each virtual user carries stable `X-Client-Id` and `X-Session-Id` headers so
 * server-side tracing can group events into clients/sessions and the following
 * high-level KPIs can be derived from the trace table:
 *   • average session duration
 *   • average session LTV
 *   • average items in cart
 *   • conversion rate from visit
 *   • average items sold per customer
 *
 * Run:
 *   mvn gatling:test -Dgatling.simulationClass=simulations.UserActivitySimulation
 *   mvn gatling:test -Dgatling.simulationClass=simulations.UserActivitySimulation \
 *     -DactivityUsers=50 -DsessionDurationSeconds=300 -DrampSeconds=30
 */
class UserActivitySimulation extends Simulation {

  import ShopConfig._

  // ── Tunables ──────────────────────────────────────────────────────────────
  private val activityUsers          = sys.props.getOrElse("activityUsers",          "10").toInt
  private val sessionDurationSeconds = sys.props.getOrElse("sessionDurationSeconds", "120").toInt
  private val activityRampSeconds    = sys.props.getOrElse("rampSeconds",            rampSeconds.toString).toInt
  private val registeredPct          = sys.props.getOrElse("registeredPct",          "0.7").toDouble
  private val addToCartPct           = sys.props.getOrElse("addToCartPct",           "0.4").toDouble
  private val checkoutPct            = sys.props.getOrElse("checkoutPct",            "0.85").toDouble

  before {
    PreChecks.assertReady(baseUrl, skipSearch = true)
    CheckoutSetup.ensureMoneyorderConfigured(baseUrl, storeCode)
  }

  // ── Feeders ───────────────────────────────────────────────────────────────

  // Leaf categories known to contain seeded products (see project memory).
  private val categoryFeeder = Array(
    "men-tshirts", "men-jeans", "men-jackets", "men-hoodies",
    "women-dresses", "women-tops", "women-jeans", "women-jackets",
    "bags", "hats", "belts"
  ).map(code => Map("categoryFriendlyUrl" -> code)).random

  // Age follows N(μ=38, σ=12), clamped to [18, 80] — models a typical adult
  // shopper population skewed toward 30s/40s rather than uniform over adults.
  private val ageMean   = 38.0
  private val ageStdDev = 12.0
  private def sampleAge(): Int = {
    val raw = ageMean + ThreadLocalRandom.current().nextGaussian() * ageStdDev
    math.min(80, math.max(18, raw.round.toInt))
  }

  private val userFeeder = Iterator.continually {
    val uid = java.util.UUID.randomUUID().toString.replace("-", "").take(20)
    val rnd = ThreadLocalRandom.current()
    Map(
      "userEmail"    -> s"p_$uid@t.example",
      "userPassword" -> "PerfTest@2024!",
      "userGender"   -> (if (rnd.nextBoolean()) "M" else "F"),
      "userAge"      -> sampleAge().toString
    )
  }

  // ── Requests ──────────────────────────────────────────────────────────────

  private val listCategories = http("Browse - GET /category")
    .get("/api/v1/category")
    .queryParam("lang", lang)
    .queryParam("store", storeCode)
    .check(status.is(200))

  private val listProductsInCategory = http("Browse - GET /products/category/{url}")
    .get("/api/v2/products/category/#{categoryFriendlyUrl}")
    .queryParam("lang", lang)
    .queryParam("store", storeCode)
    .queryParam("page", "0")
    .queryParam("count", "40")
    .check(status.is(200))
    .check(jsonPath("$.products[*].sku").findAll.saveAs("productSkus"))

  private val registerCustomer = http("Auth - POST /customer/register")
    .post("/api/v1/customer/register")
    .queryParam("store", storeCode)
    .body(StringBody(
      """{
        |  "emailAddress": "#{userEmail}",
        |  "password":     "#{userPassword}",
        |  "repeatPassword":"#{userPassword}",
        |  "firstName":    "PerfFirst",
        |  "lastName":     "PerfLast",
        |  "gender":       "#{userGender}",
        |  "billing": {
        |    "firstName":  "PerfFirst",
        |    "lastName":   "PerfLast",
        |    "address":    "1 Load Test Ave",
        |    "city":       "TestCity",
        |    "country":    "US",
        |    "zone":       "NY",
        |    "postalCode": "10001",
        |    "phone":      "5550001234"
        |  }
        |}""".stripMargin))
    .check(status.is(200))

  private val loginCustomer = http("Auth - POST /customer/login")
    .post("/api/v1/customer/login")
    .queryParam("store", storeCode)
    .body(StringBody("""{"username":"#{userEmail}","password":"#{userPassword}"}"""))
    .check(status.is(200))
    .check(jsonPath("$.token").saveAs("jwtToken"))

  private val createCart = http("Cart - POST /cart")
    .post("/api/v1/cart")
    .queryParam("store", storeCode)
    .header("Authorization", "Bearer #{jwtToken}")
    .body(StringBody("""{"product":"#{productSku}","quantity":1}"""))
    .check(status.in(200, 201))
    .check(jsonPath("$.code").saveAs("cartCode"))
    .check(jsonPath("$.total").saveAs("cartTotal"))

  private val addToExistingCart = http("Cart - PUT /cart/{code}")
    .put("/api/v1/cart/#{cartCode}")
    .queryParam("store", storeCode)
    .header("Authorization", "Bearer #{jwtToken}")
    .body(StringBody("""{"product":"#{productSku}","quantity":1}"""))
    .check(status.in(200, 201))
    .check(jsonPath("$.total").saveAs("cartTotal"))

  private val reviewCart = http("Checkout - GET /cart/{code}")
    .get("/api/v1/cart/#{cartCode}")
    .queryParam("store", storeCode)
    .header("Authorization", "Bearer #{jwtToken}")
    .check(status.is(200))
    .check(jsonPath("$.total").saveAs("cartTotal"))

  private val placeOrder = http("Checkout - POST /auth/cart/{code}/checkout")
    .post("/api/v1/auth/cart/#{cartCode}/checkout")
    .queryParam("store", storeCode)
    .header("Authorization", "Bearer #{jwtToken}")
    .body(StringBody(
      """{
        |  "customerAgreement": true,
        |  "currency": "CAD",
        |  "payment": {
        |    "paymentType":     "MONEYORDER",
        |    "paymentModule":   "moneyorder",
        |    "amount":          "#{cartTotal}",
        |    "transactionType": "AUTHORIZECAPTURE"
        |  }
        |}""".stripMargin))
    .check(status.is(200))

  // ── Building blocks ───────────────────────────────────────────────────────

  private val browseStep =
    feed(categoryFeeder)
      .exec(listCategories).pause(1, 2)
      .exec(listProductsInCategory).pause(1, 3)

  // Lazy-registers a newcomer before a cart/checkout action.
  private val ensureAuthed = doIf(session => !session.contains("jwtToken")) {
    feed(userFeeder)
      .exec(registerCustomer).pause(1, 2)
      .exec(loginCustomer).pause(1, 2)
  }

  // Picks a random product SKU from the last category listing into "productSku".
  // `findAll.saveAs` stores a Seq[String] (concrete type varies), so we read as Seq
  // and filter out any null/empty entries before sampling.
  private val pickProductSku = exec { session =>
    session("productSkus").asOption[Seq[String]].map(_.filter(s => s != null && s.nonEmpty)) match {
      case Some(skus) if skus.nonEmpty =>
        session.set("productSku", skus(ThreadLocalRandom.current().nextInt(skus.size)))
      case _ => session.remove("productSku")
    }
  }

  private val addToCartStep =
    exec(ensureAuthed)
      .doIf(session =>
        session.contains("jwtToken") &&
          session("productSkus").asOption[Seq[String]].exists(_.nonEmpty)
      ) {
        exec(session => session.set("addCount", 1 + ThreadLocalRandom.current().nextInt(5)))
          .repeat("#{addCount}", "i") {
            exec(pickProductSku)
              .doIf(session => session.contains("productSku")) {
                doIfOrElse(session => session.contains("cartCode"))(
                  exec(addToExistingCart)
                )(
                  exec(createCart)
                )
              }
              .pause(1, 2)
          }
      }

  // Gate each step on the attributes it actually needs: reviewCart only runs if
  // we have a cartCode + jwtToken; placeOrder only runs if reviewCart saved a
  // fresh cartTotal. This prevents "Attribute cartTotal's value is null" when a
  // prior cart call went KO (e.g. stale product id, 4xx on PUT /cart).
  private val checkoutStep =
    doIf(session => session.contains("cartCode") && session.contains("jwtToken")) {
      exec(reviewCart).pause(1, 2)
        .doIf(session => session.contains("cartTotal")) {
          exec(placeOrder)
        }
    }.exec(session => session.remove("cartCode").remove("cartTotal"))

  // One full iteration of the activity loop.
  private val sessionIteration =
    exec(browseStep)
      .doIf(_ => ThreadLocalRandom.current().nextDouble() < addToCartPct) {
        exec(addToCartStep)
          .doIf(session => session.contains("cartCode") &&
                           session.contains("cartTotal") &&
                           session.contains("jwtToken") &&
                           ThreadLocalRandom.current().nextDouble() < checkoutPct) {
            exec(checkoutStep)
          }
      }

  private val sessionLoop = during(sessionDurationSeconds.seconds) {
    sessionIteration
  }

  // ── Scenarios ─────────────────────────────────────────────────────────────

  private val registeredUserScn = scenario("Registered User Activity")
    .exec(initTracingHeaders)
    .feed(userFeeder)
    .exec(registerCustomer).pause(1, 2)
    .exec(loginCustomer).pause(1, 2)
    .exec(sessionLoop)

  private val newcomerScn = scenario("Newcomer User Activity")
    .exec(initTracingHeaders)
    .exec(sessionLoop)

  // ── Load profile ──────────────────────────────────────────────────────────

  private val registeredCount = math.max(1, math.round(activityUsers * registeredPct).toInt)
  private val newcomerCount   = math.max(1, activityUsers - registeredCount)

  setUp(
    registeredUserScn.inject(rampUsers(registeredCount) during activityRampSeconds.seconds),
    newcomerScn.inject(rampUsers(newcomerCount)       during activityRampSeconds.seconds)
  ).protocols(httpProtocol)
    .assertions(
      global.successfulRequests.percent.gte(80)
    )
}
