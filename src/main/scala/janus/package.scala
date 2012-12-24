import akka.dispatch.{ExecutionContextExecutorService, ExecutorServiceDelegate}
import akka.event.Logging.LogEventException
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import org.slf4j.LoggerFactory

package object janus {

  private val logger = LoggerFactory.getLogger("janus")

  private[janus] class JanusExectionContext(name: String) extends ExecutorServiceDelegate with ExecutionContextExecutorService {
    val executor: ExecutorService = new ThreadPoolExecutor(0, 64, 60L, TimeUnit.SECONDS,
      new SynchronousQueue[Runnable], new ThreadFactory {

        //TODO - make this a bit more fancy with ThreadGroups and such
        val threadNumber = new AtomicInteger(1)

        def newThread(task: Runnable): Thread = {
          val thread = Executors.defaultThreadFactory().newThread(task)
          thread.setName("janus-executor-pool-" + name + "-" + threadNumber.incrementAndGet())
          thread.setDaemon(true)
          thread
        }
      })

    def reportFailure(t: Throwable) {
      t match {
        case e: LogEventException => logger.error("Exception running task in Janus executor.", e.getCause())
        case _ => logger.error("Unexpected janus executor exception.", t)
      }
    }
  }

  def createExecutionContext(name: String = "default") = new JanusExectionContext(name)
}
