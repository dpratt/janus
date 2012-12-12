package janus

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.dispatch.{Future, Await}
import akka.util.duration._

@RunWith(classOf[JUnitRunner])
class BasicTest extends FunSuite with TestDBSupport {

  implicit val executionContext = janus.createExecutionContext

  test("Basic query test") {
    val db = Database(testDB)
    val session = db.createSession
    session.withPreparedStatement("select * from test") { ps =>
      val results = ps.executeQuery { rs =>
        rs.map(row => row[Int](1))
      }
      expect(1) {
        results.size
      }
      expect(1) {
        results.head
      }
    }
    session.close()
  }

  test("Basic transaction handling.") {

    val db = Database(testDB)

    db.withSession { session =>
      session.withTransaction { transactionalSession =>
        //insert a value
        session.withPreparedStatement("insert into test (id, name) VALUES (?,?)") { ps =>
          ps.setParam(1, 2)
          ps.setParam(2, "Test 2")
          ps.executeUpdate()
        }
      }
    }

    db.withSession { session =>
      //query for this value
      val results = session.executeQuery("select * from test where id = 2") { row =>
        row[String](2)
      }
      expect(1) {
        results.size
      }
      expect("Test 2") {
        results.head
      }
    }
  }

  test("Transaction rollback") {

    val db = Database(testDB)

    db.withSession { session =>
      val results = session.executeQuery("select count(*) from test") { row =>
        row[Int](1)
      }
      expect(1) {
        results.size
      }

      intercept[RuntimeException] {
        session.withTransaction { trans =>
          session.executeSql("insert into test (id, name) VALUES (3, 'Test Value')")
          throw new RuntimeException("Sample exception")
        }
      }
    }

    //create a new session to ensure isolation from the above operations
    db.withSession { session =>
      val newRowCount = session.executeQuery("select count(*) from test") { row =>
        row[Int](1)
      }.head
      expect(1) {
        //should only be one row - the rows inserted above should have been rolled back
        newRowCount
      }
    }
  }

  test("Async transactions") {

    val db = Database(testDB)

    val resultFuture: Future[Boolean] = db.withSession { session =>
      session.withTransaction { transactionalSession =>
        session.withPreparedStatement("insert into test (id, name) VALUES (?,?)") { ps =>
          ps.setParam(1, 2)
          ps.setParam(2, "Test 2")
          ps.executeUpdate()
        }
        //this future completes with an exception, so the transaction
        //should be rolled back
        failedExternalCall()
      }
    }

    //wait for the future to complete
    intercept[RuntimeException] {
      Await.result(resultFuture, 10 seconds)
    }

    //now make sure it's been rolled back
    db.withSession { session =>
      if(!session.executeQuery("select * from test where id = 2")(row =>"row" ).isEmpty) {
        fail("Row present!")
      }
    }
  }

  test("Nested transactions") {

    val db = Database(testDB)

    db.withSession { session =>
      intercept[RuntimeException] {
        session.withTransaction { transaction =>
          session.executeSql("insert into test (id, name) VALUES (3, 'Test 3')")
          intercept[RuntimeException] {
            session.withTransaction { nested =>
              val rowCount = session.executeQuery("select count(*) from test") { row =>
                row[Int](1)
              }.head
              expect(2) {
                rowCount
              }
              //throw an exception - this *shouldn't* roll back the transaction since we're nested
              throw new RuntimeException("This is a sample")
            }
          }
          //should still be there
          val rowCount = session.executeQuery("select count(*) from test") { row =>
            row[Int](1)
          }
          expect(2) {
            rowCount.head
          }
          //throw another exception - this *should* roll it back
          throw new RuntimeException("This is a second exception")
        }
      }
      //row should be gone
      val rowCount = session.executeQuery("select count(*) from test") { row =>
        row[Int](1)
      }
      expect(1) {
        rowCount.head
      }
    }

    //more checks - ensure that it doesn't show up in another connection either
    db.withSession { session =>
      //row should be gone
      val rowCount = session.executeQuery("select count(*) from test") { row =>
        row[Int](1)
      }
      expect(1) {
        rowCount.head
      }
    }
  }

  test("Manual rollback") {

    val db = Database(testDB)

    db.withSession { session =>
      session.withTransaction { transaction =>
        session.executeSql("insert into test (id, name) VALUES (3, 'Test 3')")
        //should still be there
        val results = session.executeQuery("select count(*) from test") { row =>
          row[Int](1)
        }
        expect(2) {
          results.head
        }
        transaction.rollback()
      }
      //row should be gone, due to manual rollback
      val rowCount = session.executeQuery("select count(*) from test") { row =>
        row[Int](1)
      }
      expect(1) {
        rowCount.head
      }
    }
  }

  def successfulExternalCall(): Future[Boolean] = {
    Future {
      Thread.sleep(500)
      true
    }
  }

  def failedExternalCall(): Future[Boolean] = {
    Future {
      Thread.sleep(500)
      throw new RuntimeException("External call failed")
    }
  }

}
