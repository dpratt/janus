package janus

import scala.concurrent._
import com.jolbox.bonecp.BoneCPDataSource
import java.util.concurrent.{ThreadFactory, SynchronousQueue, TimeUnit, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicInteger
import java.lang.Thread.UncaughtExceptionHandler
import com.typesafe.scalalogging.slf4j.Logging
import scala.concurrent.duration.Duration

trait DBConnectionInfo {
  def driverClass: String
  def username: String
  def password: String
  def jdbcUrl: String
}
case class PostgresInfo(username: String, password: String, hostname: String, port: Int, dbName: String) extends DBConnectionInfo {
  def driverClass: String = "org.postgresql.Driver"
  def jdbcUrl: String = s"jdbc:postgresql://$hostname:$port}/$dbName"
}

case class H2Info(username: String, password: String, dbPath: String) extends DBConnectionInfo {
  def driverClass: String = "org.h2.Driver"
  def jdbcUrl: String = s"jdbc:h2:file:$dbPath;MVCC=TRUE;AUTO_SERVER=TRUE"
}

case class H2InMemInfo(username: String, password: String, dbName: String) extends DBConnectionInfo {
  def driverClass: String = "org.h2.Driver"
  def jdbcUrl: String = s"jdbc:h2:mem:$dbName;MVCC=TRUE;DB_CLOSE_DELAY=-1"
}

class PooledDatabase private (pool: BoneCPDataSource, detachedEc: ExecutionContext) extends Database with Logging {
  override def withDetachedSession[T](f: (Session) => T): Future[T] = {
    future {
      withSession(f)
    }(detachedEc) //use our thread-bound executor
  }

  override protected def createSession: Session = new SimpleSession(pool.getConnection)
}

object PooledDatabase extends Logging {
  def apply(connectionInfo: DBConnectionInfo, minSize: Int, maxSize: Int, maxIdleTime: Duration): PooledDatabase = {
    require(minSize <= maxSize, "The pool min size must be smaller or equal to the max size.")

    new PooledDatabase(
      openPool(connectionInfo, minSize, maxSize, maxIdleTime),
      createExecutionContext(minSize, maxSize, maxIdleTime)
    )

  }

  private def openPool(connectionInfo: DBConnectionInfo, minConnections: Int, maxConnections: Int, maxIdle: Duration): BoneCPDataSource = {
    import com.jolbox.bonecp.BoneCPConfig
    import scala.util.control.NonFatal

    //ensure that the jdbc driver is loaded
    try {
      Class.forName(connectionInfo.driverClass)
    } catch {
      case NonFatal(e) => throw new RuntimeException("Could not load JDBC driver class!", e)
    }

    val config = new BoneCPConfig()
    config.setUsername(connectionInfo.username)
    config.setPassword(connectionInfo.password)
    config.setJdbcUrl(connectionInfo.jdbcUrl)
    config.setIdleMaxAgeInMinutes(maxIdle.toMinutes)
    config.setMinConnectionsPerPartition(minConnections)
    config.setMaxConnectionsPerPartition(maxConnections)
    config.setPoolStrategy("CACHED")

    config.setAcquireIncrement(5)
    config.setStatementsCacheSize(100)
    config.setDefaultAutoCommit(true)

    new BoneCPDataSource(config)
  }


  private def createExecutionContext(minThreads: Int, maxThreads: Int, maxIdleTime: Duration) = {
    ExecutionContext.fromExecutor(
      new ThreadPoolExecutor(minThreads, maxThreads, maxIdleTime.toMillis, TimeUnit.MILLISECONDS, new SynchronousQueue[Runnable],
        new ThreadFactory {
          val counter = new AtomicInteger(1)
          val group = new ThreadGroup(s"db-pool-${counter.getAndIncrement}")

          def newThread(r: Runnable): Thread = {
            val thread = new Thread(group, r, s"db-pool-${counter.getAndIncrement}", 0)
            thread.setDaemon(true)
            thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
              def uncaughtException(t: Thread, e: Throwable) {
                logger.error("Uncaught exception in thread {}", t.getName, e)
              }
            })
            thread
          }
        },
        new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy {
          override def rejectedExecution(r: Runnable, e: ThreadPoolExecutor) {
            logger.warn("DB execution pool has filled up! Consider allocating more threads.")
            super.rejectedExecution(r, e)
          }
        }),
      t => logger.error("Unhandled exception in DB execution context.", t))

  }


}

