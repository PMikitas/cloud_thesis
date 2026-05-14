package simulations

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._

/**
 * Simulates anonymous users browsing the Shopizer catalog.
 *
 * Covered endpoints (all public, no authentication required):
 *   GET  /api/v1/category          – list root categories
 *   GET  /api/v1/category/{id}     – view single category
 *   GET  /api/v1/products           – list products (with optional category filter)
 *   POST /api/v1/search             – full-text product search
 *   POST /api/v1/search/autocomplete – search suggestions
 *
 * Run standalone:
 *   mvn gatling:test -Dgatling.simulationClass=simulations.BrowsingSimulation
 *   mvn gatling:test -Dgatling.simulationClass=simulations.BrowsingSimulation \
 *       -DbrowseUsers=50 -DrampSeconds=60 -DholdSeconds=300
 */
class BrowsingSimulation extends Simulation {

  import ShopConfig._

  before {
    PreChecks.assertReady(baseUrl, skipSearch = true)
  }

  // ── Search terms feeder (cycles) ─────────────────────────────────────────
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

  // ── Reusable request fragments ────────────────────────────────────────────

  private val getCategories = http("GET /category")
    .get("/api/v1/category")
    .queryParam("lang", lang)
    .queryParam("store", storeCode)
    .check(status.is(200))
    .check(jsonPath("$.categories[0].id").saveAs("categoryId"))
    .check(jsonPath("$.categories[0].id").exists)

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

  // ── Scenario A: user browses categories then filters products ─────────────
  private val browseCatalog = scenario("Browse Catalog")
    .exec(getCategories)
    .pause(1, 3)
    .exec(getCategoryById)
    .pause(1, 2)
    .exec(listProductsByCategory)
    .pause(2, 5)
    .exec(listAllProducts)
    .pause(1, 3)

  // ── Scenario B: user searches for products ────────────────────────────────
  private val searchCatalog = scenario("Search Catalog")
    .feed(searchTerms)
    .exec(autocomplete)
    .pause(1, 2)
    .exec(searchProducts)
    .pause(2, 5)
    .exec(listAllProducts)
    .pause(1, 3)

  // ── Load profile ──────────────────────────────────────────────────────────
  setUp(
    browseCatalog.inject(
      rampUsers(browseUsers) during rampSeconds.seconds
    ),
    searchCatalog.inject(
      nothingFor(5.seconds),
      rampUsers(math.max(1, browseUsers / 4)) during rampSeconds.seconds
    )
  )
    .protocols(httpProtocol)
    .assertions(
      global.successfulRequests.percent.gte(95),
      global.responseTime.percentile3.lte(2000)
    )
}
