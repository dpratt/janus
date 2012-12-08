Janus
=====


Janus is a library focused on simple data access with the ability to interleave database transactions
with asynchronous Future-based invocations.

A quick example -

```scala

def externalCall(id: String) : Future[String]

val db = Database(dataSource)
db.withSession { session =>
  session.withTransaction { transaction =>
    session.executeSql("insert into test (id, name) VALUES (1, 'Test Value')")
    externalCall("Test Value")
  }
}
```

The code above creates a connection to a database, and then opens a Session (the logical equivalent to a connection)
against this database. We then open a new transactional boundary and then invoke an external, asynchronous Future-based
web service. Since the return value of the transaction block is Future based, we do not immediately commit the transaction.
Instead, Janus notices this case, and will then either roll back or commit the transaction only after the Future
itself completes.

Note - if the return value of the transaction block isn't Future based, the transaction is immediately comitted when
the block is evaluated. Of course, the transaction is rolled back if the block either throws an exception or rollback()
is manually called on the transaction definition itself.