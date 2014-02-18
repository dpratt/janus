import collection.mutable.ArrayBuffer
import concurrent.ExecutionContext
import java.sql.ResultSet
import org.slf4j.LoggerFactory

package object janus {

  import java.util.concurrent.{ThreadFactory, SynchronousQueue, TimeUnit, ThreadPoolExecutor}
  import java.util.concurrent.atomic.AtomicInteger

  /**
   * The default ExecutionContext for async access to Janus.
   */
  private[janus] lazy val defaultExecutionContext = {
    val log = LoggerFactory.getLogger("janus.executionContext")

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


  private[janus] def copyResultSet(resultSet: java.sql.ResultSet): Seq[Row] = {

    val meta = Metadata(resultSet)
    val columnRange = 1 to meta.columns.size
    def data(rs: java.sql.ResultSet): IndexedSeq[Any] = {
      for (i <- columnRange) yield rs.getObject(i)
    }

    val rows = IndexedSeq.newBuilder[Row]
    try {
      while(resultSet.next()) {
        rows += Row(meta, data(resultSet))
      }
      rows.result()
    } finally {
      resultSet.close()
    }
  }
}
