package simulations

import java.net.{HttpURLConnection, URL}
import scala.io.Source
import scala.util.{Try, Using}

/**
 * One-time server-side setup needed before checkout-bearing load tests can
 * succeed against a fresh Shopizer instance.
 *
 * Shopizer stores payment modules per merchant in the MERCHANT_CONFIGURATION
 * table under key `PAYMENT_MODULES`. Until one is configured, every
 * `POST /api/v1/auth/cart/{code}/checkout` fails with
 * "No payment module configured" wrapped in a 503.
 *
 * We pick `moneyorder` because its module impl (MoneyOrderPayment) has no
 * external dependencies — it only validates that an `address` integration key
 * is set and then returns a stub transaction on authorizeAndCapture.
 */
object CheckoutSetup {

  private val adminEmail    = "admin@shopizer.com"
  private val adminPassword = "password"

  def ensureMoneyorderConfigured(baseUrl: String, storeCode: String): Unit = {
    println("\n========== Checkout prerequisites ==========")

    loginAdmin(baseUrl) match {
      case Some(token) =>
        configureMoneyorder(baseUrl, storeCode, token) match {
          case Right(_)    => println("[OK]   Payment module 'moneyorder' is configured")
          case Left(error) => println(s"[WARN] Could not configure 'moneyorder': $error (continuing — may already be set)")
        }
      case None =>
        println("[WARN] Admin login failed — skipping payment module configuration")
    }

    println("============================================\n")
  }

  private def loginAdmin(baseUrl: String): Option[String] = {
    val body = s"""{"username":"$adminEmail","password":"$adminPassword"}"""
    httpPost(s"$baseUrl/api/v1/private/login", body, authHeader = None) match {
      case scala.util.Success((200, rb)) =>
        val idx = rb.indexOf("\"token\"")
        if (idx < 0) None
        else {
          val start = rb.indexOf('"', idx + 8) + 1
          val end   = rb.indexOf('"', start)
          if (start > 0 && end > start) Some(rb.substring(start, end)) else None
        }
      case _ => None
    }
  }

  private def configureMoneyorder(baseUrl: String, storeCode: String, adminToken: String): Either[String, Unit] = {
    val payload =
      """{"code":"moneyorder","active":true,"defaultSelected":true,""" +
      """"integrationKeys":{"address":"1 Load Test Ave, TestCity"},""" +
      """"integrationOptions":{}}"""

    httpPost(
      s"$baseUrl/api/v1/private/modules/payment?store=$storeCode",
      payload,
      authHeader = Some(s"Bearer $adminToken")
    ) match {
      case scala.util.Success((s, _)) if s >= 200 && s < 300 => Right(())
      case scala.util.Success((s, b))                        => Left(s"HTTP $s: ${b.take(200)}")
      case scala.util.Failure(ex)                            => Left(ex.getMessage)
    }
  }

  private def httpPost(url: String, body: String, authHeader: Option[String], timeoutMs: Int = 5000): Try[(Int, String)] = Try {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setConnectTimeout(timeoutMs)
    conn.setReadTimeout(timeoutMs)
    conn.setRequestProperty("Accept", "application/json")
    conn.setRequestProperty("Content-Type", "application/json")
    authHeader.foreach(h => conn.setRequestProperty("Authorization", h))
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
}
