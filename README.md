# SimpleDB RDBMS implementation

* Written in `Java 23`
* Maven `v3.9.9` with the [wrapper](https://maven.apache.org/wrapper/)
* Compiled to native executable using [GraalVM](https://www.graalvm.org/)
* Uses [virtual threads](https://docs.oracle.com/en/java/javase/23/core/virtual-threads.html) and [structured concurrency](https://docs.oracle.com/en/java/javase/23/core/structured-concurrency.html)

## Build & run

### Standard maven

* Build self-executable jar file
```bash
./mvnw clean package
```

* Run application (sequential/parallel)
Pay attention that we also need to provide `--enable-preview` during runtime because we have used 
[Structured Concurrency](https://docs.oracle.com/en/java/javase/23/core/structured-concurrency.html) which is in 
a preview mode for java 23.
```bash
java --enable-preview -jar target/simpledb-0.0.1-SNAPSHOT.jar
```

### Native image

* Build native image using maven `native` profile

If you're using Windows make sure to have installed [Visual Studio 2022](https://visualstudio.microsoft.com/downloads/).
It's required to compile native images.

```bash
./mvnw clean package -Pnative
```

* Run native executable (Unix/Windows)
```bash
./target/simpledb
./target/simpledb.exe
```

## Quality checks

### OWASP check dependencies for vulnerabilities
* Run OWASP dependency checker
```bash
./mvnw org.owasp:dependency-check-maven:check
```

# References
* [Database Design and Implementation: Second Edition](https://www.amazon.com/Database-Design-Implementation-Data-Centric-Applications/dp/3030338355/ref=sr_1_1?crid=G1I4279Q48AJ&dib=eyJ2IjoiMSJ9.7tga9E_bl8msW5-z9Pn4KkSbTSae4Bu_vtviIO4dFLZUNMTGTuygTO_T_krVTD9tg7dm3DTEw16-XNSN_sn6g2pMTNPHiQf7F9SvNamu6IpQFEpd_SlvYggwq0UQKDLsoKyglsuUQck3DOVd14KOx0c5KzMhEvgabxVZN6NFmf419blzCyx2jKrEab1M-YcX93zg32ahNqvknuhi9rNiX5qQ2Sg4RXpuCCtGCLvE-VE.SZATG01thHfOxfLUK_2DvokHIKBqn6-h_BWpUPC0Lpc&dib_tag=se&keywords=database+design+and+implementation&qid=1721130039&sprefix=database+design+and+implementation%2Caps%2C194&sr=8-1) 
