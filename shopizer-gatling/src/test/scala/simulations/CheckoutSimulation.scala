package simulations

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._

/**
 * Simulates the full authenticated purchase flow:
 *   register → login → browse products → add to cart → checkout → view order
 *
 * Each virtual user registers with a unique e-mail address so runs are
 * repeatable without any database cleanup between executions.
 *
 * Covered endpoints:
 *   POST /api/v1/customer/register            – create account
 *   POST /api/v1/customer/login               – obtain JWT
 *   GET  /api/v1/products                     – pick a product
 *   POST /api/v1/cart                         – create cart + add item
 *   PUT  /api/v1/cart/{code}                  – add second item
 *   GET  /api/v1/cart/{code}                  – review cart
 *   POST /api/v1/auth/cart/{code}/checkout    – place order (authenticated)
 *   GET  /api/v1/auth/orders/{id}             – confirm order
 *
 * Run standalone:
 *   mvn gatling:test -Dgatling.simulationClass=simulations.CheckoutSimulation
 *   mvn gatling:test -Dgatling.simulationClass=simulations.CheckoutSimulation \
 *       -DcheckoutUsers=10 -DrampSeconds=60
 */
class CheckoutSimulation extends Simulation {

  import ShopConfig._

  before {
    PreChecks.assertReady(baseUrl, skipSearch = true)
  }

  // ── Unique-user feeder ────────────────────────────────────────────────────
  private val userFeeder = Iterator.continually {
    val uid = java.util.UUID.randomUUID().toString.replace("-", "")
    Map(
      "userEmail"    -> s"perf_$uid@shoptest.example",
      "userPassword" -> "PerfTest@2024!"
    )
  }

  // ── Step 1: register ──────────────────────────────────────────────────────
  private val registerCustomer = http("POST /customer/register")
    .post("/api/v1/customer/register")
    .queryParam("store", storeCode)
    .body(StringBody(
      """{
        |  "emailAddress": "#{userEmail}",
        |  "password":     "#{userPassword}",
        |  "repeatPassword":"#{userPassword}",
        |  "firstName":    "PerfFirst",
        |  "lastName":     "PerfLast",
        |  "billing": {
        |    "firstName":    "PerfFirst",
        |    "lastName":     "PerfLast",
        |    "address":      "1 Load Test Ave",
        |    "city":         "TestCity",
        |    "country":      "US",
        |    "zone":         "NY",
        |    "postalCode":   "10001",
        |    "phone":        "5550001234"
        |  }
        |}""".stripMargin
    ))
    .check(status.is(200))

  // ── Step 2: login, save JWT ───────────────────────────────────────────────
  private val loginCustomer = http("POST /customer/login")
    .post("/api/v1/customer/login")
    .queryParam("store", storeCode)
    .body(StringBody("""{"username":"#{userEmail}","password":"#{userPassword}"}"""))
    .check(status.is(200))
    .check(jsonPath("$.token").saveAs("jwtToken"))

  // ── Step 3: pick a product ────────────────────────────────────────────────
  private val fetchFirstProduct = http("GET /products")
    .get("/api/v1/products")
    .queryParam("page", "0")
    .queryParam("count", "20")
    .queryParam("lang", lang)
    .queryParam("store", storeCode)
    .check(status.is(200))
    .check(jsonPath("$.products[0].id").saveAs("productId"))
    .check(jsonPath("$.products[0].id").exists)

  // ── Step 4: create cart with first product ────────────────────────────────
  private val createCart = http("POST /cart")
    .post("/api/v1/cart")
    .queryParam("store", storeCode)
    .body(StringBody("""{"product":"#{productId}","quantity":1}"""))
    .check(status.is(200))
    .check(jsonPath("$.code").saveAs("cartCode"))

  // ── Step 5: add a second item ─────────────────────────────────────────────
  private val addItemToCart = http("PUT /cart/{code}")
    .put("/api/v1/cart/#{cartCode}")
    .queryParam("store", storeCode)
    .body(StringBody("""{"product":"#{productId}","quantity":2}"""))
    .check(status.is(200))

  // ── Step 6: review cart ───────────────────────────────────────────────────
  private val reviewCart = http("GET /cart/{code}")
    .get("/api/v1/cart/#{cartCode}")
    .queryParam("store", storeCode)
    .check(status.is(200))

  // ── Step 7: checkout (FREE payment – works without payment gateway) ────────
  private val placeOrder = http("POST /auth/cart/{code}/checkout")
    .post("/api/v1/auth/cart/#{cartCode}/checkout")
    .queryParam("store", storeCode)
    .header("Authorization", "Bearer #{jwtToken}")
    .body(StringBody(
      """{
        |  "customerAgreement": true,
        |  "payment": {
        |    "paymentType":     "FREE",
        |    "transactionType": "AUTHORIZECAPTURE"
        |  }
        |}""".stripMargin
    ))
    .check(status.is(200))
    .check(jsonPath("$.id").saveAs("orderId"))

  // ── Step 8: confirm order ─────────────────────────────────────────────────
  private val viewOrder = http("GET /auth/orders/{id}")
    .get("/api/v1/auth/orders/#{orderId}")
    .queryParam("store", storeCode)
    .header("Authorization", "Bearer #{jwtToken}")
    .check(status.is(200))

  // ── Full purchase scenario ────────────────────────────────────────────────
  val fullPurchase = scenario("Full Purchase")
    .exec(initTracingHeaders)
    .feed(userFeeder)
    .exec(registerCustomer)
    .pause(1, 2)
    .exec(loginCustomer)
    .pause(1, 2)
    .exec(fetchFirstProduct)
    .pause(2, 4)
    .exec(createCart)
    .pause(1, 2)
    .exec(addItemToCart)
    .pause(1, 3)
    .exec(reviewCart)
    .pause(2, 5)
    .exec(placeOrder)
    .pause(1, 2)
    .exec(viewOrder)

  // ── Load profile ──────────────────────────────────────────────────────────
  setUp(
    fullPurchase.inject(
      rampUsers(checkoutUsers) during rampSeconds.seconds
    )
  )
    .protocols(httpProtocol)
    .assertions(
      global.successfulRequests.percent.gte(90),
      global.responseTime.percentile3.lte(5000)
    )
}
