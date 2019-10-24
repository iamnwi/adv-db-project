# Distributed Database

## Authors
- Jialiang Cao(jc8343)
- Waii Ng(win205)

## Description
This is a Fall 2019 Advance Database course project. For project requirements, please see `project-requirements` PDF.

A draft of the distributed Database architecture.
![Architecture](imgs/arch_draft.png)

## Data
- 10 sites
- 20 data entries: x1, ..., x20
  - even indexed entires are in all sites
  - odd indexed entires are in site 1 + (index mod 10)

## Algorithms
### 1. Available copies
- For replicated entries, i.e. even indexed entries
- Use 2 phase locking(read, write locks)

**Data Structure Concerns**
1. Need to tell if a replicated entry of a recovered site has been written or not. If it isn't, then we cannot read it
     - Possible solution:
       1. A field on the data table of each site
2. Commit check

---
### 2. Multi-version read
- For read-only transactions

**Data Structure Concerns**
1. We need to keep all version(or some version that needed) of each value
      - Possible solution:
         1. An extra table for data value snapshots. Whenever a read-only transaction begins, we take a snapshots(with timestamp) and add to the extra table

---
### 3. Wait-die
- For deadlock avoidance
- One optimization

**Data Structure Concerns**
1. For each lock, we need to tell the begin time of the transaction that holding it
     - Possible solution:
       1. A field on lock table to record the transaction begin time

## Components
### Transaction Manager
   - Functions:
     1. Record the begin time of each transaction
     2. Commit check
     3. Abort

---
### DM(Disk Manager? Distribution Manager?)
  