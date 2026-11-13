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

## Processvy

En process kan ha en processvy (stegindikatorn) som visar handläggaren var i processen en uppgift befinner sig. Mallen innehåller ett exempel med två delar:

- Processvy-länken `processvy.url` i [application.properties](src/main/resources/application.properties), som skickas med i regelanropen.
- Processbeskrivningen [processbeskrivning.json](src/main/resources/processbeskrivning.json) bredvid `template_process.bpmn`, som listar processens steg och vilka aktivitets-ID:n som hör till varje steg. Stegen hämtas med `GET /processvy/steg?aktivitetId={uuid}`.

Anpassa länken och processbeskrivningen till processen. Varje aktivitets-ID i BPMN-filen ska höra till ett innersta steg, och `TemplateProcessbeskrivningTest` går sönder om något saknas. Mallens BPMN-fil heter `template_process.bpmn` så att den inte krockar med `template.bpmn` i mallens regelunderprocess, som också hamnar på classpathen. Formatet beskrivs i [rimfrost-framework-process](https://github.com/Forsakringskassan/rimfrost-framework-process#processvy).

Båda delarna är valfria. Ska processen inte ha någon processvy tar du bort `processvy.url`, `processbeskrivning.json`, `TemplateProcessbeskrivningTest`, testet `TestProcessvySteg`, kontrollerna av `processvyUrl` i `TemplateSmokeTest` och beroendet till framework-process test-jar (`<classifier>tests</classifier>`) i `pom.xml`. Processen startar och skickar regelanrop som vanligt, bara utan processvy.

### Understeg

Ett steg kan innehålla understeg i stället för aktiviteter, i högst två nivåer under det översta steget. Mallens exempel har steget `fas` med understeget `regel`, som har mallens enda aktivitet. Ett steg har antingen `steg` eller `aktiviteter`, aldrig båda, och varje aktivitets-ID hör till ett innersta steg. Lägg till fler understeg i `fas` när processen har fler aktiviteter, eller gör stegen platta (bara `aktiviteter` på översta nivån) om processen inte behöver understeg. Processvyn räknar status för varje nivå: ett steg med understeg är `PAGAR` om något understeg pågår, `KLAR` om alla är klara och annars `KOMMANDE`. Smoke-testet `TestProcessvySteg` kontrollerar id och status för `fas` och `regel`, så ändra det när processbeskrivningen ändras.

### Validering

`TemplateProcessbeskrivningTest` validerar `processbeskrivning.json` med ramverkets inläsare, samma som läser filen när processen startar, och körs med de andra testerna i bygget och i `maven-ci`. Inläsaren kontrollerar att de obligatoriska fälten finns och att inga andra fält finns, att aktivitets-ID:n är UUID, att ett steg har antingen `steg` eller `aktiviteter`, att det finns högst tre nivåer, att id är unika och att ett aktivitets-ID bara hör till ett steg. Testet kontrollerar dessutom att `processId` stämmer med BPMN-filen och att varje aktivitets-ID i BPMN-filen hör till ett innersta steg. Valideringen följer ramverksversionen i `pom.xml` och behöver inte uppdateras separat. JSON-schemat i rimfrost-framework-process beskriver formatet för en framtida generator av processbeskrivningar och används inte av processen.

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