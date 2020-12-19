
# Reactive JOOQ - an R2DBC adapter for Spring Boot

![GitHub tag (latest SemVer)](https://img.shields.io/github/tag/gofabian/spring-boot-data-r2dbc-jooq)
![tests](https://github.com/gofabian/spring-boot-data-r2dbc-jooq/workflows/tests/badge.svg)

This library integrates JOOQ into Spring Data R2DBC.

Usage:

```java
// fetch queries
var selectQuery = dslContext.selectCount().from(BOOK_TABLE);
Flux<Record1<Integer>> flux = ReactiveJooq.fetch(selectQuery);
Mono<Record1<Integer>> mono = ReactiveJooq.fetchOne(selectQuery);
Mono<Record1<Integer>> mono = ReactiveJooq.fetchAny(selectQuery);
Mono<Integer> mono = ReactiveJooq.fetchCount(selectQuery);
Mono<Boolean> mono = ReactiveJooq.fetchExists(selectQuery);

// fetch queries with generated JOOQ tables
var tableQuery = dslContext.selectFrom(BOOK_TABLE);
Flux<BookRecord> flux = ReactiveJooq.fetch(selectQuery);

// manipulating queries
var insertQuery = dslContext.insertInto(table("book"), field("name")).values("book");
Mono<Integer> mono = ReactiveJooq.execute(insertQuery);

// record manipulation
var record = dslContext.newRecord(BOOK_TABLE);
Mono<Integer> mono = ReactiveJooq.store(record);
Mono<Integer> mono = ReactiveJooq.insert(record);
Mono<Integer> mono = ReactiveJooq.update(record);
Mono<Integer> mono = ReactiveJooq.delete(record);
Mono<Void> mono = ReactiveJooq.refresh(record);
```


## Getting Started with Maven

Add this library and a database driver (e. g. h2) to your `pom.xml`:

```xml
<dependency>
    <groupId>de.gofabian</groupId>
    <artifactId>spring-boot-data-r2dbc-jooq</artifactId>
    <version>0.3.0</version>
</dependency>
<dependency>
    <groupId>io.r2dbc</groupId>
    <artifactId>r2dbc-h2</artifactId>
    <version>0.8.4.RELEASE</version>
</dependency>
```

If you use Spring Boot the library auto-configures a DSLContext with the correct SQL dialect for you. You can autowire
the DSLContext in your code. 

If you do not use Spring Boot make sure that `R2dbcJooqAutoConfiguration` is detected by Spring.


## Example with Spring Boot

```java
@Service
public class ReactiveBookService {
    @Autowired DSLContext dslContext;

    public Mono<BookPojo> getBookById(Long id) {
        Query query = dslContext.selectFrom(BOOK_TABLE).where(BOOK_TABLE.ID.eq(id));
        return ReactiveJooq.fetchOne(query).map(r -> r.into(BookPojo.class));
    }

    public Mono<Integer> createBook(BookPojo book) {
        BookRecord record = dslContext.newRecord(BOOK_TABLE, book);
        return ReactiveJooq.insert(record);
    }

    public Mono<Integer> deleteBookById(Long id) {
        Query query = dslContext.deleteFrom(BOOK_TABLE).where(BOOK_TABLE.ID.eq(id));
        return ReactiveJooq.execute(query);
    }
}
```


## Support

- Spring Boot 2.4
- H2, MySQL, PostgreSQL
- Java 8+
- Spring @Transactional annotation


## Realization status

| JOOQ | Reactive JOOQ |
| --- | --- |
| `query.execute()` -> `int` | `ReactiveJooq.execute(query)` -> `Mono<Integer>` |
| `query.fetch()` -> `Result<R>` | `ReactiveJooq.fetch(query)` -> `Flux<R>` |
| `query.fetchOne()` -> `R` | `ReactiveJooq.fetchOne(query)` -> `Mono<R>` |
| `query.fetchAny()` -> `R` | `ReactiveJooq.fetchAny(query)` -> `Mono<R>` |
| `dslContext.fetchExists(query)` -> `boolean` | `ReactiveJooq.fetchExists(query)` -> `Mono<Boolean>` |
| `dslContext.fetchCount(query)` -> `int` | `ReactiveJooq.fetchCount(query)` -> `Mono<Integer>` |
| `query.returning(...).fetch()` -> `Result<R>` | `ReactiveJooq.executeReturning(query)` -> `Flux<R>` |
| `query.returning(...).fetchOne()` -> `Result<R>` | `ReactiveJooq.executeReturningOne(query)` -> `Mono<R>` |
| `record.store()` -> `int` | `ReactiveJooq.store(record)` -> `Mono<Integer>` |
| `record.insert()` -> `int` | `ReactiveJooq.insert(record)` -> `Mono<Integer>` |
| `record.update()` -> `int` | `ReactiveJooq.update(record)` -> `Mono<Integer>` |
| `record.delete()` -> `int` | `ReactiveJooq.delete(record)` -> `Mono<Integer>` |
| `record.refresh()` | `ReactiveJooq.refresh(record)` -> `Mono<Void>` |


## Restrictions

The auto-configured DSLContext does not have a JDBC connection. It is only intended to generate SQL strings which is 
given to the R2DBC database client for execution.

Do not use JOOQ methods that require a JDBC connection (every method that calls the database)! Use the `ReactiveJooq` 
methods instead.

Incomplete list of methods that will not work:

- `DslContext.fetchXxx()`
- `DslContext.executeXxx()`
- `DslContext.transactionXxx()`
- `Record.store()`
- `Record.insert()`
- `Record.update()`
- `Record.refresh()`
- `Record.delete()`

Further unsupported features:

- `RecordListener`
- `ExecuteListener`
- `TransactionListener`
- `settings.setExecuteWithOptimisticLocking(true)`
- `settings.setExecuteWithOptimisticLockingExcludeUnversioned(true)`
- `settings.setUpdateRecordVersion(true)`
- `settings.setUpdateRecordTimestamp(true)`
- `settings.setUpdatablePrimaryKeys(true)`
- `settings.setQueryPoolable(...)`
- `settings.setQueryTimeout(...)`
- `settings.setMaxRows(...)`
- `settings.setFetchSize(...)`
- ...


## JOOQ/R2DBC type converters

JOOQ and the R2DBC drivers have built-in support for specific SQL data types. For example: JOOQ 
puts `JSON` data into an instance of `org.jooq.JSON`, the r2dbc-postgresql driver has an own 
wrapper `io.r2dbc.postgresql.codec.Json`. That means we need a converter for the `JSON <-> Json` 
conversion.

The default converter supports the `JSON` conversion for MySQL and PostgreSQL and can be found at:

```java
    dslContext.configuration().data("converter")
```

You can override this value if you want to add custom converters:

```java
    Converter myConverter = ...    

    Converter converter = (Converter) dslContext.configuration().data("converter");
    converter = new CompositeConverter(new Converter[]{converter, myConverter});
    dslContext.configuration().data("converter", converter);
```

## Release process

(0) Prerequisites:

Signing key must be available:

    $ gpg --list-keys

OSSRH credentials must be available:

    $ cat ~/.m2/settings.xml
    <settings>
      <servers>
        <server>
          <id>ossrh</id>
          <username>your-jira-id</username>
          <password>your-jira-pwd</password>
        </server>
      </servers>
      <profiles>
        <profile>
          <activation>
            <activeByDefault>true</activeByDefault>
          </activation>
          <properties>
            <gpg.keyname>your-keyname</gpg.keyname>
          </properties>
        </profile>
      <profiles>
    </settings>


(1) Manual steps to prepare release, e. g. update version in Readme file

    $ nano Readme.md
    $ git add . && git commit -m 'Bump version'

(2) Prepare release: update version in pom.xml, git tag

     $ JAVA_HOME=/usr/lib/jvm/java-8-openjdk mvn release:prepare

(3) Perform release: build artifact, upload to OSS staging, release to Maven Central

    $  JAVA_HOME=/usr/lib/jvm/java-8-openjdk mvn release:perform

(4) Publish release to github, too

    $ git push --tags

(5) Release should appear in Maven Central after some minutes:

https://repo1.maven.org/maven2/de/gofabian/spring-boot-data-r2dbc-jooq/


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
