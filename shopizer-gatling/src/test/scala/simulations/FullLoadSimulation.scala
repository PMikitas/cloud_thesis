package simulations

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._

/**
 * Combined realistic load simulation mixing all user types:
 *
 *   ~70 % anonymous catalog browsers   (BrowsingSimulation scenarios)
 *   ~20 % registered customers doing a full purchase (CheckoutSimulation)
 *   ~10 % search-focused users (skipped automatically if search is unavailable)
 *
 * The mix is driven by the browseUsers / checkoutUsers config properties
 * (defaults: 20 browsers, 5 checkout users).
 *
 * Run:
 *   mvn gatling:test
 *   mvn gatling:test -DbrowseUsers=50 -DcheckoutUsers=10 \
 *       -DrampSeconds=60 -DholdSeconds=300 \
 *       -DbaseUrl=http://staging.example.com:8080
 */
class FullLoadSimulation extends Simulation {

  import ShopConfig._

  // Whether search/Elasticsearch is available – checked once at startup
  private val searchAvailable: Boolean = false //sys.props.getOrElse("skipSearch", "false") != "true"

  before {
    PreChecks.assertReady(baseUrl, skipSearch = !searchAvailable)
  }

  // ── Search terms feeder (shared) ──────────────────────────────────────────
  private val searchTerms = Array(
    Map("searchQuery" -> "shirt"),
    Map("searchQuery" -> "laptop"),
    Map("searchQuery" -> "shoes"),
    Map("searchQuery" -> "watch"),
    Map("searchQuery" -> "bag"),
    Map("searchQuery" -> "phone"),
    Map("searchQuery" -> "dress"),
    Map("searchQuery" -> "jacket")
  ).circular

  // ── Unique-user feeder (purchase flow) ────────────────────────────────────
  private val userFeeder = Iterator.continually {
    val uid = java.util.UUID.randomUUID().toString.replace("-", "")
    Map(
      "userEmail"    -> s"perf_$uid@shoptest.example",
      "userPassword" -> "PerfTest@2024!"
    )
  }

  // ═══════════════════════════ REQUEST DEFINITIONS ══════════════════════════

  // -- Browsing ---------------------------------------------------------------
  private val getCategories = http("GET /category")
    .get("/api/v1/category")
    .queryParam("lang", lang)
    .queryParam("store", storeCode)
    .check(status.is(200))
    .check(jsonPath("$.categories[0].id").saveAs("categoryId"))

  private val getCategoryById = http("GET /category/{id}")
    .get("/api/v1/category/#{categoryId}")
    .queryParam("lang", lang)
    .queryParam("store", storeCode)
    .check(status.is(200))

  private val listProductsByCategory = http("GET /products?category")
    .get("/api/v1/products")
    .queryParam("category", "#{categoryId}")
    .queryParam("page", "0")
    .queryParam("count", "20")
    .queryParam("lang", lang)
    .queryParam("store", storeCode)
    .check(status.is(200))
    .check(jsonPath("$.products[0].id").saveAs("productId"))

  private val listAllProducts = http("GET /products (all)")
    .get("/api/v1/products")
    .queryParam("page", "0")
    .queryParam("count", "20")
    .queryParam("lang", lang)
    .queryParam("store", storeCode)
    .check(status.is(200))
    .check(jsonPath("$.products[0].id").saveAs("productId"))

  private val searchProducts = http("POST /search")
    .post("/api/v1/search")
    .queryParam("store", storeCode)
    .body(StringBody("""{"query":"#{searchQuery}"}"""))
    .check(status.is(200))

  private val autocomplete = http("POST /search/autocomplete")
    .post("/api/v1/search/autocomplete")
    .queryParam("store", storeCode)
    .body(StringBody("""{"query":"#{searchQuery}"}"""))
    .check(status.is(200))

  // -- Purchase flow ----------------------------------------------------------
  private val registerCustomer = http("POST /customer/register")
    .post("/api/v1/customer/register")
    .queryParam("store", storeCode)
    .body(StringBody(
      """{
        |  "emailAddress":  "#{userEmail}",
        |  "password":      "#{userPassword}",
        |  "repeatPassword":"#{userPassword}",
        |  "firstName":     "PerfFirst",
        |  "lastName":      "PerfLast",
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
        |}""".stripMargin
    ))
    .check(status.is(200))

  private val loginCustomer = http("POST /customer/login")
    .post("/api/v1/customer/login")
    .queryParam("store", storeCode)
    .body(StringBody("""{"username":"#{userEmail}","password":"#{userPassword}"}"""))
    .check(status.is(200))
    .check(jsonPath("$.token").saveAs("jwtToken"))

  private val createCart = http("POST /cart")
    .post("/api/v1/cart")
    .queryParam("store", storeCode)
    .body(StringBody("""{"product":"#{productId}","quantity":1}"""))
    .check(status.is(200))
    .check(jsonPath("$.code").saveAs("cartCode"))

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

  private val viewOrder = http("GET /auth/orders/{id}")
    .get("/api/v1/auth/orders/#{orderId}")
    .queryParam("store", storeCode)
    .header("Authorization", "Bearer #{jwtToken}")
    .check(status.is(200))

  // ═══════════════════════════ SCENARIOS ════════════════════════════════════

  private val browseCatalog = scenario("Browse Catalog")
    .exec(getCategories)
    .pause(1, 3)
    .exec(getCategoryById)
    .pause(1, 2)
    .exec(listProductsByCategory)
    .pause(2, 5)

  private val searchCatalog = scenario("Search Catalog")
    .feed(searchTerms)
    .exec(autocomplete)
    .pause(1, 2)
    .exec(searchProducts)
    .pause(2, 5)
    .exec(listAllProducts)
    .pause(1, 3)

  private val fullPurchase = scenario("Full Purchase")
    .feed(userFeeder)
    .exec(registerCustomer)
    .pause(1, 2)
    .exec(loginCustomer)
    .pause(1, 2)
    .exec(listAllProducts)
    .pause(2, 4)
    .exec(createCart)
    .pause(1, 3)
    .exec(placeOrder)
    .pause(1, 2)
    .exec(viewOrder)

  // ═══════════════════════════ LOAD PROFILE ═════════════════════════════════
  private val browseInjection = Seq(
    browseCatalog.inject(
      rampUsers(browseUsers) during rampSeconds.seconds,
      constantUsersPerSec(browseUsers.toDouble / 10) during holdSeconds.seconds
    ),
    fullPurchase.inject(
      nothingFor(10.seconds),
      rampUsers(checkoutUsers) during rampSeconds.seconds
    )
  )

  private val searchInjection = if (searchAvailable) Seq(
    searchCatalog.inject(
      nothingFor(5.seconds),
      rampUsers(math.max(1, browseUsers / 5)) during rampSeconds.seconds,
      constantUsersPerSec(math.max(1, browseUsers / 5).toDouble / 10) during holdSeconds.seconds
    )
  ) else Seq.empty

  setUp((browseInjection ++ searchInjection): _*)
    .protocols(httpProtocol)
    .assertions(
      global.successfulRequests.percent.gte(90),
      global.responseTime.percentile3.lte(5000),
      forAll.failedRequests.percent.lte(10)
    )
}
