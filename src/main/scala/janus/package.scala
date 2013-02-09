import concurrent.ExecutionContext
import java.sql.ResultSet
import org.slf4j.LoggerFactory

package object janus {

  import java.util.concurrent.{ThreadFactory, SynchronousQueue, TimeUnit, ThreadPoolExecutor}
  import java.util.concurrent.atomic.AtomicInteger

  /**
   * The default ExecutionContext for async access to Janus.
   */
  implicit lazy val defaultExecutionContext = {
    //TODO - make the thread pool params configurable
    ExecutionContext.fromExecutor(
      new ThreadPoolExecutor(5, 60, 30, TimeUnit.SECONDS, new SynchronousQueue[Runnable],
        new ThreadFactory {
          val counter = new AtomicInteger(1)
          val group = new ThreadGroup("janus-pool-" + counter.getAndIncrement)

          def newThread(r: Runnable): Thread = {
            val thread = new Thread(group, r, "janus-thread-" + counter.getAndIncrement, 0)
            thread.setDaemon(true)
            thread
          }
        },
        new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy {
          override def rejectedExecution(r: Runnable, e: ThreadPoolExecutor) {
            log.warn("Janus execution pool has filled up! Consider allocating more threads.")
            super.rejectedExecution(r, e)
          }
        }),
      t => log.error("Error in thread pool.", t))
  }

  private val log = LoggerFactory.getLogger("janus")

  private[janus] def withResultSet[A](rs: ResultSet, f: Traversable[Row] => A): A = {
    import JdbcRow._
    try {
      f(convertResultSet(rs))
    } finally {
      rs.close()
    }
  }

  def convertResultSet(resultSet: java.sql.ResultSet): Traversable[Row] = {

    val meta = Metadata(resultSet)
    val columnRange = 1 to meta.columns.size
    def data(rs: java.sql.ResultSet) = {
      for (i <- columnRange) yield rs.getObject(i)
    }

    new Traversable[Row] {
      def foreach[U](f: Row => U) {
        if (resultSet.getType != ResultSet.TYPE_FORWARD_ONLY) {
          resultSet.beforeFirst()
        } else {
          log.warn("Cannot reset position of ResultSet. Repeat enumeration of rows not supported.")
        }
        while (resultSet.next()) {
          f(JdbcRow(data(resultSet), meta))
        }
      }
    }
  }

}
