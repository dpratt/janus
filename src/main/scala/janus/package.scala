import akka.dispatch.ExecutionContext
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

package object janus {

  def createExecutionContext = ExecutionContext.fromExecutor(
    new ThreadPoolExecutor(0, 64, 60L, TimeUnit.SECONDS,
      new SynchronousQueue[Runnable], new ThreadFactory {

        //TODO - make this a bit more fancy with ThreadGroups and such
        val threadNumber = new AtomicInteger(1)

        def newThread(task: Runnable): Thread = {
          val thread = Executors.defaultThreadFactory().newThread(task)
          thread.setName("default-unstuck-pool-" + threadNumber.incrementAndGet())
          thread.setDaemon(true)
          thread
        }
      })
  )



}
