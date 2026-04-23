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