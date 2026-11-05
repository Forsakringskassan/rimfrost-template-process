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

Anpassa länken och processbeskrivningen till processen. Varje aktivitets-ID i BPMN-filen ska höra till ett innersta steg, och `TemplateProcessbeskrivningTest` går sönder om något saknas. Mallens BPMN-fil heter `template_process.bpmn` så att den inte krockar med `template.bpmn` i mallens regelunderprocess, som också hamnar på classpathen. Formatet beskrivs i [rimfrost-framework-process](https://github.com/Forsakringskassan/rimfrost-framework-process#processvy).

Båda delarna är valfria. Ska processen inte ha någon processvy tar du bort `PROCESSVY_URL`, `processbeskrivning.json` och `TemplateProcessbeskrivningTest`, testet `TestProcessvySteg` och kontrollerna av `processvyUrl` i `TemplateContainerSmokeIT`, jobbet `validate-processbeskrivning` i `maven-ci` och beroendet till framework-process test-jar (`<classifier>tests</classifier>`) i `pom.xml`. Processen startar och skickar regelanrop som vanligt, bara utan processvy.

### Understeg

Ett steg kan innehålla understeg i stället för aktiviteter, i högst två nivåer under det översta steget. Mallens exempel har en fas (`fas`) med ett understeg (`regel`) som har mallens enda aktivitet. Ett steg har antingen `steg` eller `aktiviteter`, aldrig båda, och varje aktivitets-ID hör till ett innersta steg. Lägg till fler understeg i fasen när processen har fler aktiviteter, eller gör stegen platta (bara `aktiviteter` på översta nivån) om processen inte behöver understeg. Processvyn räknar status för varje nivå: en fas är `PAGAR` om något understeg pågår, `KLAR` om alla är klara och annars `KOMMANDE`. Smoke-testet `TestProcessvySteg` kontrollerar fasens och understegets id och status, så ändra det när processbeskrivningen ändras.

### Validering i CI

`maven-ci` validerar `processbeskrivning.json` mot ramverkets JSON Schema genom att anropa workflowet `validate-processbeskrivning.yaml` i rimfrost-framework-process. Schemat kontrollerar fälten, att aktivitets-ID:n är UUID och att ett steg har antingen `steg` eller `aktiviteter` i högst tre nivåer. Unika id och att ett aktivitets-ID bara hör till ett steg kontrolleras när processen startar och i `TemplateProcessbeskrivningTest`. Ange samma ref av ramverket i `uses:` och `framework-ref`.

## Bygg projektet

`./mvnw -s settings.xml clean compile`

## Bygg och testa projektet

`./mvnw -s settings.xml clean verify`

## Bygg docker image för lokal testning

`./mvnw -s settings.xml clean package`

## Github workflows

Tre github workflows är inkluderade i projektet, maven-ci, maven-release och smoke-test. maven-ci validerar också `processbeskrivning.json`, se [Validering i CI](#validering-i-ci).

maven-release skapar som del av sitt flöde en docker image.
Den publiceras till försäkringskassans [repository](https://github.com/Forsakringskassan/repository).

## Exempel implementation
Se [rimfrost-process-vah](https://github.com/Forsakringskassan/rimfrost-process-vah) för en färdig implementation av en flödesprocess.