Janus
=====


Janus is a library focused on simple data access with the ability to interleave database transactions
with asynchronous Future-based invocations. Classic transactional models on the JVM typically rely on a single
thread-bound transaction context or connection, but this can prove difficult when dealing with asychronous
external resources. To work around this you either need to implement your external (or otherwise Future based) calls
as blocking, which tends to be not as robust and elegant. Janus does not utilize the concept of a thread bound
transaction, but rather the convention is to source a Session implicitly.

A quick example -

```scala

import akka.dispatch.Future
import janus.{Session, Database}
import javax.sql.DataSource

trait ExternalRepositorySupport {
  def addUser(id: Long, password: String): Future[String]
  def deleteUser(id: Long): Future[Boolean]
}

trait DataSourceSupport {
  def dataSource: DataSource
}

case class User(id: Long, name: String)

object User {
  def insertUser(user: User)(implicit session: Session): User = {
    session.withTransaction { transaction =>
      session.withPreparedStatement("insert into test (id, name) VALUES (?, ?)") { ps =>
        ps.setParam[Long](1, user.id)
        ps.setParam[String](2, user.name)
        if(ps.executeUpdate() != 1) {
          throw new RuntimeException("Could not insert user!")
        }
      }
      user
    }
  }
}

trait UserRepository {
  this: ExternalRepositorySupport with DataSourceSupport =>
  val db = Database(dataSource)

  def createUser(id: Long, name: String, password: String): Future[User] = {
    db.withSession { implicit session =>
      session.withTransaction { transaction =>
        val user = User.insertUser(User(id, name))
        addUser(id, password) recoverWith {
          //handle the manual rollback of the external call.
          case e: Throwable => {
            //External call failed - clean it up
            deleteUser(id)
            //re-throw the error
            throw e
          }
        } map { passwordResult =>
          //Dont' care about the String returned from the external service - just return the User
          user
        }
      }
    }
  }
}


```

The code above creates a connection to a database, and then opens a Session (the logical equivalent to a connection)
against this database. We then open a new transactional boundary and then invoke an external, asynchronous Future-based
web service. Since the return value of the transaction block is Future based, we do not immediately commit the transaction.
Instead, Janus notices this case, and will then either roll back or commit the transaction only after the Future
itself completes. We do have to handle non-database rollbacks by ourselves, typically using the 'recover' or 'recoverWith'
methods on the ultimate returned Future.

Note - if the return value of the transaction block isn't Future based, the transaction is immediately comitted when
the block is evaluated. Of course, the transaction is rolled back if the block either throws an exception or rollback()
is manually called on the transaction definition itself.