package spark.jobserver.util

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, Cache => GuavaCache}
import com.typesafe.config.Config
import ooyala.common.akka.metrics.MetricsWrapper

object GuavaCacheUtils {
  def buildCache[K,V](cacheSize: Int, ttlSeconds: Long): GuavaCache[K,V] = {
    CacheBuilder.newBuilder
      .maximumSize(cacheSize)
      .expireAfterAccess(ttlSeconds, TimeUnit.SECONDS)
      .recordStats()
      .build()
      .asInstanceOf[GuavaCache[K,V]]
  }

  def buildCache[K,V](config: Config): GuavaCache[K,V] =
    buildCache(
      config.getInt("spark.jobserver.job-result-cache-size"),
      config.getLong("spark.jobserver.job-result-cache-ttl-seconds")
    )

  //pimp my cache
  class WrappedCache[K,V](cache: GuavaCache[K,V]) {
    def withMetrics(klass: Class[_]): GuavaCache[K,V] = {
      MetricsWrapper.newGauge(klass, "cache-hit",  cache.stats.hitCount())
      MetricsWrapper.newGauge(klass, "cache-miss", cache.stats.missCount())
      MetricsWrapper.newGauge(klass, "cache-eviction", cache.stats.evictionCount())
      MetricsWrapper.newGauge(klass, "cache-request", cache.stats.requestCount())
      cache
    }
  }

  implicit def toWrappedCache[K,V](cache: GuavaCache[K,V]): WrappedCache[K,V] = new WrappedCache[K,V](cache)
}