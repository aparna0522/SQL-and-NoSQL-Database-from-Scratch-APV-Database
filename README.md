# APV-Database
Implemented a Relational Database System (using files and folder structure) and NoSQL database (using dynamic hashing and linear probing) from scratch, along with a CLI and a natural language for a user to interact and retrieve answers to their queries.

## How to run this project?
MacOS (Linux): ```rlwrap python3 cli.py``` \
Windows: ```cli.py```

## Project Demo:
Project Demo Link: <a href = "https://player.vimeo.com/video/888146630?badge=0&amp;autopause=0&amp;quality_selector=1&amp;player_id=0&amp;app_id=58479%22%20frameborder=%220%22%20allow=%22autoplay;%20fullscreen;%20picture-in-picture"> Link </a>

## Folder/File Descriptions
- cli.py: 
    Python file to run the SQL and NoSQL Commands of our query language and get the output. 
- config.json:
    Configuartion variables to run the nosql_src.py file
- init.sh
    Initial Setup to build the project
- nosql_src.py:
    Python code for the nosql database system. Contains functions for CRUD operations on NoSQL database and external calls to PageManager.exe (PageManager.o - depending on the OS) to carry out operations like Joining, Aggregation, Grouping, Ordering.
- sql_workspace:
  - RelationalDBAPI.exe (RelationalDB.o - depending on the OS): Executable for running the relational database queries on the CLI.
  - On executing the program: (New Folders created)
    - databases:
      Folder to store all the databases being created in relational SQL. Contains separate folders for every attribute in the schema of the table. Every table has a meta file, storing the schema of the entire table, and the constraints (primary key) for the given table. 
    - temp:
      Folder that helps in processing the queries, and deletes the contents after the execution of the query. Operations like join, ordering, group by, search results, etc. use this folder to save the system's results, before printing it to the terminal. On print, these files and folders get deleted and are inaccessible to the end user. All the data is divided in chunks, and the program loads the chunks into the memory, thus helping in storing and fast retreival of the tables, joining, ordering tables, with data as huge as 20000-40000 entries per table.
- sql_src:
  - attributes.h: 
    Header file that has BTree Node, functions to operate on this node, and perform CRUD operations, range search, etc. All Nodes are internally saved as files on the disk and retrieved if required, helping in reduced memory usage for huge files.
  - table.h: 
    Header file that helps in table operations like creation, deletion, contains helper functions for insertion, deletion of entries, searches, joins, groupby, etc.
  - common.h
    Configuartion variables to run the relational database.
  - pages.h
    Processing and enable printing of queries outputs for joins, aggregations, group by and ordering. This is used by both SQL and NoSQL database design for operations on the database. 
  - relationalAPI.cpp
    APIs to enable cli.py to call the cpp functions from python. 
  - extras:
    - menuBasedCLI.cpp: Allows to independently run relational database using this as the main.cpp file. 
    - PageManager.cpp: cpp file using pages.h interally as its header file, provides an API interface for python to execute temp folder processings
- resources: 
  databases used to show the syntax for the Query language (below):
  - SQL: (Games)
    - attendance.csv 
    - games.csv
    - standings.csv
  - NoSQL: (Country)
    - country-by-abbreviation.json
    - country-by-continent.json
    - country-by-currency-name.json
    - country-by-currency-code.json

## SQL Commands
Please make sure to have the resources folder downloaded before running the following commands: 

#### Define Table Operation
- Define Table standings:
  ```
  define table standings with [["idx","integer","primaryKey"],["team", "string"], ["team_name", "string"], ["year", "integer"],["wins", "integer"],["loss", "integer"],["points_for", "integer"],["points_against", "integer"],["points_differential", "integer"],["margin_of_victory", "float"],["strength_of_schedule", "float"],["simple_rating", "float"],["offensive_ranking", "float"],["defensive_ranking", "float"], ["playoffs", "string"],["sb_winner", "string"]];
  ```
- Define Table attendance:
  ```
  define table attendance with [["idx","integer","primaryKey"], ["team", "string"], ["team_name", "string"], ["year", "integer"], ["total", "string"], ["home", "string"], ["away", "string"], ["week", "string"], ["weekly_attendance", "string"]];
  ```
- Define Table games:
  ```
  define table games with [["idx", "integer","primaryKey"], ["year","integer"], ["week","string"], ["home_team","string"],["away_team","string"], ["winner","string"], ["tie","string"], ["day","string"], ["date","string"], ["time", "string"], ["pts_win","integer"],["pts_loss","integer"], ["yds_win","integer"], ["turnover_win","integer"], ["yds_loss","integer"], ["turnover_loss","integer"], ["home_team_name","string"],["home_team_city","string"], ["away_team_name","string"], ["away_team_city","string"]];
  ```
  
#### Insert Bulk CSV
- Load all the data from the CSVs to the table:
  ```
  load data in standings with "../resources/SQL/standings.csv" generate primaryKeyValues provided headers;
  load data in attendance with "../resources/SQL/attendance.csv" generate primaryKeyValues provided headers;
  load data in games with "../resources/SQL/games.csv" generate primaryKeyValues provided headers;
  ```

#### Find Operation
- Search for all the entries in the database:
  ```
  find all from standings;
  find all from attendance;
  find all from games;
  ```
  
- Search for particular entries in the database using "provided" (where) clause:
  ```
  find all from games provided idx > 300;
  find all from attendance provided team_name = "Vikings";
  ```
  
- Project particular entries in the database using "provided" (where) clause:
  ```
  find ["team", "team_name", "wins", "year"] from standings provided sb_winner = "Won Superbowl";
  find ["home_team", "tie", "pts_win", "pts_loss"] from games provided day = "Sat";
  ```

#### Fill Operation
- Fill (insert) data into the tables
  ```
  fill table standings with values [5000, "India", "CSK", 2022, 56, 12, 31, 52, 12, 6.3, 5.2, 5.9, 5.1, 7.3, 8.1, "MII"];
  find ["team", "wins", "loss"] from standings provided team = "India";
  ```

#### Edit Operation
- Edit (update) existing data in the table
  ```
  edit table standings with values team_name = 'RCB', year = 2023, wins = 0 provided idx = 5000;
  ```

#### Remove Operation
- remove (delete) existing entry in the table
  ```
  remove from table standings provided idx = 5000
  ```

#### Cluster On and aggregation Operation 
- Group By And Aggregation Operation:
  ```
  find ["CNT(team_name)", "SUM(wins)", "SUM(loss)"] from standings cluster on team;
  ```

#### Sorting Operation 
- Group By And Aggregation Operation:
  ```
  find ["team_name", "team"] from standings sorting team_name DESC;
  ```

#### Merge Operation
- Merge two tables 
  ```
  merge (find ["team", "team_name"] from standings) as t with (find ["team", "wins", "loss"] from standings) as s on s.team = t.team; // Self join on same table...
  merge (find ["team", "wins", "loss"] from standings) as s with (find ["away_team_name", "tie"] from games) as t on s.team_name = t.away_team_name;  // two different tables being used having team name (s) = away_team_name (t) as common attributes; Both tables have a lot of data, initial processing for join takes couple extra seconds. 
  ```

#### Database Modularity: Mixing various operations (Group By, and Ordering by using where Clause and projecting specific columns)
- Mixing cluster, ordering projection operations
  ```
  find ["CNT(team_name)", "SUM(wins)", "SUM(loss)"] from standings provided wins > 10 cluster on team sorting SUM(wins) DESC;
  find ["away_team_name", "SUM(pts_win)"] from games sorting away_team_name DESC cluster by pts_win;
  ```

## NoSQL Commands 

#### Insert Bulk CSV
- Load all the data from the CSVs to the table:
  ```
  load data resources/NoSQL/country-by-continent.json;
  load data resources/NoSQL/country-by-abbreviation.json;
  load data resources/NoSQL/country-by-currency-name.json;
  load data resources/NoSQL/country-by-currency-code.json;
  ```

#### Find Operation
- Search for all the entries in the database:
  ```
  find all from country-by-continent;
  ```
  
- Search for particular entries in the database using "provided" (where) clause:
  ```
  find all from country-by-continent provided continent = Antarctica;
  ```
  
- Project particular entries in the database using "provided" (where) clause:
  ```
  find ["continent", "country"] from country-by-continent provided continent in ["Asia", "Africa"] sorting continent asc;
  ```

#### Fill Operation
- Fill (insert) data into the tables
  ```
  fill table country-by-abbreviation with values {"country": "Verdansk", "abbreviation": "VDK", "id": 246};
  fill table country-by-abbreviation with values {"country": "Wakanda", "abbreviation": "WNK", "id": 247};
  fill table country-by-abbreviation with values {"country": "Urzikstan", "abbreviation": "URS", "id": 248};
  ```

#### Edit Operation
- Edit (update) existing data in the table
  ```
  edit table country-by-abbreviation with values abbreviation = WND provided country = Wakanda;
  ```

#### Remove Operation
- remove (delete) existing entry in the table
  ```
  remove from table country-by-abbreviation provided id in [246, 247];
  remove from table country-by-abbreviation provided id = 248;
  ```

#### Cluster On and aggregation Operation 
- Group By And Aggregation Operation:
  ```
  find ["CNT(country)", "SUM(population)"] from country-by-continent cluster on continent sorting continent asc;
  ```

#### Sorting Operation 
- Group By And Aggregation Operation:
  ```
  find ["CNT(country)", "SUM(population)"] from country-by-continent cluster on continent sorting CNT(country) desc;
  find ["CNT(country)", "SUM(population)"] from country-by-continent cluster on continent sorting continent desc;
  ```

#### Merge Operation
- Merge two tables 
  ```
  merge (find all from country-by-currency-code) with (find all from country-by-currency-name) on country = country;
  merge (find ["country", "continent"] from country-by-continent provided continent = Asia) with (find ["country", "abbreviation"] from country-by-abbreviation) on country = country sorting s.continent asc;
  ```

#### Database Modularity: Mixing various operations (Group By, and Ordering by using where Clause and projecting specific columns)
- Mixing cluster, ordering projection operations
  ```
  find ["CNT(country)", "SUM(population)"] from country-by-continent cluster on continent sorting SUM(population) desc;
  ```
