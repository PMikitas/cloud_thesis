package simulations

import java.net.{HttpURLConnection, URL}
import scala.io.Source
import scala.util.{Try, Using}

/**
 * Synchronous pre-flight checks executed before any Gatling scenario starts.
 *
 * Call PreChecks.assertReady() inside a simulation's before {} block.
 * If any check fails the simulation is aborted with a clear error message.
 *
 * Checks performed:
 *   1. API is reachable (HTTP 200 on /actuator/health with status UP)
 *   2. Database has at least one category
 *   3. Database has at least one product
 *   4. Search endpoint is responding (Elasticsearch availability)
 */
object PreChecks {

  private case class Result(ok: Boolean, message: String)
  private val DefaultTimeoutMs = 15000
  private val ProductCheckCategorySlugs = Seq("men-tshirts", "women-tops", "bags")

  private def httpGet(url: String, timeoutMs: Int = DefaultTimeoutMs): Try[(Int, String)] = Try {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setConnectTimeout(timeoutMs)
    conn.setReadTimeout(timeoutMs)
    conn.setRequestProperty("Accept", "application/json")
    conn.setRequestMethod("GET")
    val status = conn.getResponseCode
    val body = Using(Source.fromInputStream(
      if (status < 400) conn.getInputStream else conn.getErrorStream
    ))(_.mkString).getOrElse("")
    conn.disconnect()
    (status, body)
  }

  private def httpPost(url: String, body: String, timeoutMs: Int = DefaultTimeoutMs): Try[(Int, String)] = Try {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setConnectTimeout(timeoutMs)
    conn.setReadTimeout(timeoutMs)
    conn.setRequestProperty("Accept", "application/json")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.getOutputStream.write(body.getBytes("UTF-8"))
    val status = conn.getResponseCode
    val responseBody = Using(Source.fromInputStream(
      if (status < 400) conn.getInputStream else conn.getErrorStream
    ))(_.mkString).getOrElse("")
    conn.disconnect()
    (status, responseBody)
  }

  private def responseHasProducts(body: String): Boolean =
    body.contains("\"products\"") && !body.contains("\"products\":[]")

  private def checkHealth(baseUrl: String): Result = {
    httpGet(s"$baseUrl/actuator/health") match {
      case scala.util.Success((200, body)) if body.contains("\"UP\"") =>
        Result(ok = true, "Health: UP")
      case scala.util.Success((status, body)) =>
        Result(ok = false, s"Health check failed – HTTP $status: $body")
      case scala.util.Failure(ex) =>
        Result(ok = false, s"Cannot reach $baseUrl – ${ex.getMessage}")
    }
  }

  private def checkCategories(baseUrl: String): Result = {
    httpGet(s"$baseUrl/api/v1/category?page=0&count=5") match {
      case scala.util.Success((200, body)) if !body.contains("\"categories\":[]") =>
        Result(ok = true, "Categories: data present")
      case scala.util.Success((200, _)) =>
        Result(ok = false,
          "No categories found in the database. Load seed / demo data before running load tests.")
      case scala.util.Success((status, body)) =>
        Result(ok = false, s"Categories endpoint returned HTTP $status: $body")
      case scala.util.Failure(ex) =>
        Result(ok = false, s"Categories endpoint error: ${ex.getMessage}")
    }
  }

  private def checkProducts(baseUrl: String): Result = {
    val v1ProductsUrl = s"$baseUrl/api/v1/products?page=0&count=1&lang=en&store=DEFAULT"

    httpGet(v1ProductsUrl) match {
      case scala.util.Success((200, body)) if responseHasProducts(body) =>
        Result(ok = true, "Products: data present")
      case scala.util.Success((status, body)) =>
        Result(ok = false, s"Products endpoint returned HTTP $status: $body")
      case _ =>
        val categoryFallbacks = ProductCheckCategorySlugs.map { slug =>
          val url = s"$baseUrl/api/v2/products/category/$slug?page=0&count=1&lang=en&store=DEFAULT"
          slug -> httpGet(url)
        }

        categoryFallbacks.collectFirst {
          case (slug, scala.util.Success((200, body))) if responseHasProducts(body) =>
            Result(ok = true, s"Products: data present via category listing ($slug)")
        }.getOrElse {
          val firstFailure =
            categoryFallbacks.collectFirst {
              case (slug, scala.util.Success((status, body))) =>
                s"$slug returned HTTP $status: $body"
              case (slug, scala.util.Failure(ex)) =>
                s"$slug error: ${ex.getMessage}"
            }.getOrElse("all category fallbacks returned empty product lists")

          Result(
            ok = false,
            s"Products endpoint error: v1 listing unavailable and category fallback failed ($firstFailure)"
          )
        }
    }
  }

  private def checkSearch(baseUrl: String): Result = {
    httpPost(s"$baseUrl/api/v1/search", """{"query":"test"}""") match {
      case scala.util.Success((200, _)) =>
        Result(ok = true, "Search (Elasticsearch): available")
      case scala.util.Success((status, body)) =>
        Result(ok = false,
          s"Search endpoint returned HTTP $status – Elasticsearch may be down: $body")
      case scala.util.Failure(ex) =>
        Result(ok = false, s"Search endpoint error: ${ex.getMessage}")
    }
  }

  /**
   * Run all pre-checks and throw [[IllegalStateException]] if any fail.
   * Call this from a simulation's before {} block.
   */
  def assertReady(baseUrl: String, skipSearch: Boolean = false): Unit = {
    println("\n========== Pre-flight system checks ==========")

    val checks = Seq(
      checkHealth(baseUrl),
      checkCategories(baseUrl),
      checkProducts(baseUrl)
    ) ++ (if (skipSearch) Seq.empty else Seq(checkSearch(baseUrl)))

    val failures = checks.filterNot(_.ok)

    checks.foreach { r =>
      val icon = if (r.ok) "[OK]  " else "[FAIL]"
      println(s"$icon ${r.message}")
    }

    println("==============================================\n")

    if (failures.nonEmpty) {
      val reasons = failures.map("  • " + _.message).mkString("\n")
      throw new IllegalStateException(
        s"Pre-flight checks failed – aborting simulation:\n$reasons"
      )
    }
  }
}
