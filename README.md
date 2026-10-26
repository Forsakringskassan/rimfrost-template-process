# Mallprojekt för Rimfrost process

Det här är ett mall projekt för att skapa en övergripande flödesprocess för handläggning i rimfrost-projektet.

En flödesprocess är en kogito-process för att producera ett resultat baserat på resultat från en eller flera underprocesser, där underprocesserna består av underordnade flödesprocesser eller regelprocesser.

För regel-baserade processer, se [template för regelprocess](https://github.com/Forsakringskassan/rimfrost-template-regel-subprocess/tree/main).

## Minimum konfiguration av utvecklingsmiljö

Projektet förväntar sig att jdk (java version 21 eller högre),
docker och maven är installerat på systemet samt att
miljövariablerna **GITHUB_ACTOR** och **GITHUB_TOKEN** är
konfigurerade.

Notera att det GITHUB token som används förväntas ha repo access
konfigurerad för att kunna hämta vissa projekt beroenden.

## TODOS
Projektet innehåller ett antal TODO kommentarer som beskriver konfiguration som bör ändras
och beroenden som bör ersättas. Se t.ex. src/main/resources/application.properties
och pom.xml för exempel på dessa.

## Persistens

Processinstanserna sparas i en PostgreSQL-databas med Kogitos JDBC-persistens
(`kie-addons-quarkus-persistence-jdbc`). Det gör att pågående handläggningar
överlever omstarter av tjänsten och kan fortsätta när svar från underprocesser
kommer in.

Konfigurationen finns i `src/main/resources/application.properties`:

| Property | Beskrivning |
|---|---|
| `kogito.persistence.type=jdbc` | Aktiverar JDBC-persistens av processinstanser. |
| `kogito.persistence.proto.marshaller=false` | Processvariabler serialiseras utan protobuf-schema. |
| `kogito.persistence.query.timeout.millis` | Timeout för databasfrågor. |
| `kie.flyway.enabled=true` | Kogitos tabeller skapas och migreras av Flyway vid uppstart (`kie-addons-quarkus-flyway`). |
| `quarkus.datasource.db-kind=postgresql` | Databastyp. |

I prod-profilen läses anslutningen från miljövariabler som måste sättas när tjänsten startas.
I dev- och test-profilerna startar Quarkus Dev Services en PostgreSQL-container automatiskt.

| Miljövariabel | Beskrivning |
|---|---|
| `DB_URL` | JDBC-url, t.ex. `jdbc:postgresql://host:5432/databas` |
| `DB_USERNAME` | Databasanvändare |
| `DB_PASSWORD` | Lösenord för databasanvändaren |

## Bygg projektet

`./mvnw -s settings.xml clean compile`

## Bygg och testa projektet

`./mvnw -s settings.xml clean verify`

## Bygg docker image för lokal testning

`./mvnw -s settings.xml clean package`

## Github workflows

Tre github workflows är inkluderade i projektet, maven-ci, maven-release och smoke-test.

maven-release skapar som del av sitt flöde en docker image.
Den publiceras till försäkringskassans [repository](https://github.com/Forsakringskassan/repository).

## Exempel implementation
Se [rimfrost-process-vah](https://github.com/Forsakringskassan/rimfrost-process-vah) för en färdig implementation av en flödesprocess.