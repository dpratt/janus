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
      ps.executeQuery { rs =>
        rs.next()
        expect(1) {
          rs.getValue[Int](1)
        }
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
      session.executeQuery("select * from test where id = 2") { rs =>
      //ensure it's there
        if(!rs.next()) {
          fail("Row not present!")
        } else {
          expect("Test 2") {
            rs.getValue[String](2)
          }
        }
      }
    }
  }

  test("Transaction rollback") {

    val db = Database(testDB)

    db.withSession { session =>
      session.executeQuery("select count(*) from test") { rs =>
        rs.next()
        expect(1) {
          rs.getValue[Int](1)
        }
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
      val newRows = session.executeQuery("select count(*) from test") { rs =>
        rs.next()
        rs.getValue[Int](1)
      }
      expect(1) {
        //should only be one row - the rows inserted above should have been rolled back
        newRows
      }
    }
  }

  test("Advanced transactions") {

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
      session.executeQuery("select * from test where id = 2") { rs =>
        //ensure it's not there
        if(rs.next()) {
          fail("Row present!")
        }
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
              session.executeQuery("select count(*) from test") { rs =>
                rs.next()
                expect(2) {
                  rs.getValue[Int](1)
                }
              }
              //throw an exception - this *shouldn't* roll back the transaction since we're nested
              throw new RuntimeException("This is a sample")
            }
          }
          //should still be there
          session.executeQuery("select count(*) from test") { rs =>
            rs.next()
            expect(2) {
              rs.getValue[Int](1)
            }
          }
          //throw another exception - this *should* roll it back
          throw new RuntimeException("This is a second exception")
        }
      }
    }
    db.withSession { session =>
      //row should be gone
      session.executeQuery("select count(*) from test") { rs =>
        rs.next()
        expect(1) {
          rs.getValue[Int](1)
        }
      }
    }
  }

  test("Manual rollback") {

    val db = Database(testDB)

    db.withSession { session =>
      session.withTransaction { transaction =>
        session.executeSql("insert into test (id, name) VALUES (3, 'Test 3')")
        //should still be there
        session.executeQuery("select count(*) from test") { rs =>
          rs.next()
          expect(2) {
            rs.getValue[Int](1)
          }
        }
        transaction.rollback()
      }
      //row should be gone, due to manual rollback
      session.executeQuery("select count(*) from test") { rs =>
        rs.next()
        expect(1) {
          rs.getValue[Int](1)
        }
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