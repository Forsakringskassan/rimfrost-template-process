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

- Processvy-länken `PROCESSVY_URL` i [application.properties](src/main/resources/application.properties), som skickas med i regelanropen.
- Processbeskrivningen [processbeskrivning.json](src/main/resources/processbeskrivning.json) bredvid `template_process.bpmn`, som listar processens steg och vilka aktivitets-ID:n som hör till varje steg. Stegen hämtas med `GET /processvy/steg?aktivitetId={uuid}`.

Anpassa länken och processbeskrivningen till processen. Varje aktivitets-ID i BPMN-filen ska höra till ett steg, och `TemplateProcessbeskrivningTest` går sönder om något saknas. Mallens BPMN-fil heter `template_process.bpmn` så att den inte krockar med `template.bpmn` i mallens regelunderprocess, som också hamnar på classpathen. Formatet beskrivs i [rimfrost-framework-process](https://github.com/Forsakringskassan/rimfrost-framework-process#processvy).

Båda delarna är valfria. Ska processen inte ha någon processvy tar du bort `PROCESSVY_URL`, `processbeskrivning.json` och `TemplateProcessbeskrivningTest`, testet `TestProcessvySteg` och kontrollerna av `processvyUrl` i `TemplateContainerSmokeIT`, och beroendet till framework-process test-jar (`<classifier>tests</classifier>`) i `pom.xml`. Processen startar och skickar regelanrop som vanligt, bara utan processvy.

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