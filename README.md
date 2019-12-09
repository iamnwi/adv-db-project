![](https://github.com/iamnwi/adv-db-project/workflows/Java%20CI/badge.svg?branch=master)

# RepCRec Distributed Database
Fall 2019 Advanced Database Course Project

## Authors
- Jialiang Cao(jc8343)
- Waii Ng(win205)

## Description
This is a Fall 2019 Advanced Database course project. For project requirements, please see `project-requirements` PDF.

## How to run

We used **reprozip** to pack our project to allow anyone reproducing this project using **reprounzip** easily.

### Reprounzip
```
reprounzip directory setup repcrecdb.rpz repcrecdb
reprounzip directory run repcrecdb
```
You should see the detail results of running all the test cases we provided.

You also can run this project using Jar after you **reprounzip** it.
```
cd repcrecdb/root/vagrant/adv-db-project
java -jar build/libs/repcrecdb.jar [input-file]
```
All test cases will run if no input file is given.

### Gradle Build

Tests will run when building with Gradle
```
git clone https://github.com/iamnwi/adv-db-project.git
cd adv-db-project
gradle clean
gradle build
```

### Run Jar

```
cd adv-db-project
java -jar build/libs/repcrecdb.jar [input-file]
```
All test cases will run if no input file is given.

## Architecture
The architecture of this RepCRec Distributed Database is as follow.
![Architecture](imgs/project-arch.png)

## Data
- 10 sites
- 20 data entries: x1, ..., x20
  - even indexed entires are in all sites
  - odd indexed entires are in site 1 + (index mod 10)

## Algorithms
- Available copies
- Multi-version read
- Deadlock detection

## Components
For component details, please see `RepCRec-design-doc` PDF.
  
