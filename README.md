# APV-Database
Architected and implemented a SQL and NoSQL database, while providing a CLI to run queries using a newly devised query language to interact and retrieve results.

## Key Features of the project:
1. Loads and processes huge datasets (where number of rows >>> number of columns) in optimal memory allocations. \
For instance: Consider a file of size 750kB, and the table having 20 columns and about 10k rows. If we consider there are 50 rows in every chunk, and every value of has size of 8Bytes, so two input files, and one output file would in all take 3 * 20 * 50 * 8 Bytes = 24000 Bytes = 24 kilobytes. Thus, (24/750) kilobytes would be about 3%. This means that for a file as huge as 750 kB, only 3% data would be loaded into main memory at the same time for processing.
2. Enables CLI support for relational as well as nosql database. Type chdb in the terminal to change the database type from sql to nosql and viceversa.
3. Provides cross-platform support for Mac, Linux and Windows OS
4. Uses BTree for Relational Database Structure, enabling faster query retrieval (search). Uses external merge sort algorithm to order huge amounts of data with limited memory allocations. NoSQL databases use dyanmic hashing with linear probing and chaining to make query search faster and more efficient.

## How to run this project?

MacOS (Linux): 
```
brew install rlwrap
./init.sh
rlwrap python3 cli.py
```
Note: rlwrap is a 'readline wrapper', a small utility that uses the GNU Readline library to allow the editing of keyboard input for any command. rlwrap is an optional utility. 

Windows: ```python cli.py```

## Project Demo
Relational Database Demonstration: 

https://github.com/aparna0522/APV-Database/assets/36110304/75e9b181-8589-4bce-9029-4d843b0bd6de

NoSQL Database Demonstration:

https://github.com/aparna0522/APV-Database/assets/36110304/9b90895d-2afe-4213-b85c-f45277492cab


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

## SQL and NoSQL on Students database (Small database for grading purposes) 

### SQL commands
- List Tables
```
list tables; 
```
- Define table for "students"
```
define table students with [["sid", "integer", "primaryKey"], ["name", "string"], ["numberOfCourses", "integer"], ["department", "string"], ["school", "string"]];
```
- Define table for "instructors"
```
define table instructors with [["iid", "integer", "primaryKey"], ["name","string"], ["monthlyIncome", "integer"], ["department","string"],["school", "string"]];
```
- List Tables
```
list tables; 
```
- Describe tables
```
list table students;
```
- Entries for the "students" table
```
fill table students with values [1, "John Smith", 3, "Computer Science", "Viterbi School of Engineering"];
fill table students with values [2, "Emily Johnson", 2, "Electrical Engineering", "Viterbi School of Engineering"];
fill table students with values [3, "Michael Davis", 2, "Mechanical Engineering", "Viterbi School of Engineering"];
fill table students with values [4, "Sophia Miller", 5, "Modern World Arts and Design", "Roski School of Art and Design"];
fill table students with values [5, "Daniel White", 5, "Urban Planning", "Sol Price School of Public Policy"];
fill table students with values [6, "Olivia Brown", 4, "Elements of Music", "Thornton School of Music"];
fill table students with values [7, "Ethan Wilson", 7, "Biomedical Engineering", "Keck School of Medicine"];
fill table students with values [8, "Ava Martinez", 3, "Sociology", "Dornsife College of Letters, Arts and Sciences"];
fill table students with values [9, "Matthew Lee", 1, "Business Analytics", "Marshall School of Business"];
fill table students with values [10, "Emma Taylor", 5, "Environmental Engineering", "Viterbi School of Engineering"];
```

- Entries for the "instructors" table
```
fill table instructors with values [1, "Professor Anderson", 43000,"Computer Science", "Viterbi School of Engineering"];
fill table instructors with values [2, "Professor Rodriguez", 43000, "Electrical Engineering", "Viterbi School of Engineering"];
fill table instrufctors with values [3, "Professor Turner", 41000, "Mechanical Engineering", "Viterbi School of Engineering"];
fill table instructors with values [4, "Professor Carter", 23000, "Chemical Engineering", "Viterbi School of Engineering"];
fill table instructors with values [5, "Professor Bennett", 53000, "Civil Engineering", "Viterbi School of Engineering"];
fill table instructors with values [6, "Professor Davis", 25000, "Biomedical Engineering", "Keck School of Medicine"];
fill table instructors with values [7, "Professor Smith", 10000, "Computer Science", "Viterbi School of Engineering"];
fill table instructors with values [8, "Professor Johnson", 25000, "Business Analytics", "Marshall School of Business"];
fill table instructors with values [9, "Professor White",25000, "Industrial and Systems Engineering", "Viterbi School of Engineering"];
fill table instructors with values [10, "Professor Miller",25000, "Environmental Studies", "Viterbi School of Engineering"];
fill table instructors with values [11, "Professor Sarah", 45000, "Elements of Music", "Thornton School of Music"];
fill table instructors with values [12, "Professor Taylor", 85000, "Business Analytics", "Marshall School of Business"];
fill table instructors with values [13, "Professor Andrew", 55000, "Psychology", "Dornsife College of Letters, Arts and Sciences"];
fill table instructors with values [14, "Professor Johnathan", 22000, "Sociology", "Dornsife College of Letters, Arts and Sciences"];
fill table instructors with values [15, "Professor Suruchi", 89000, "Computer Science", "Viterbi School of Engineering"];
fill table instructors with values [16, "Professor Rahila", 22000, "Urban Planning", "Sol Price School of Public Policy"];
```

- Show all the entries in "students" table
```
find all from students;
```

- Show all entries in "instructors" table
```
find all from instructors;
```

- Load data in students tables 
```
load data in students with "../resources/SQL/students.csv" provided headers;
```

- Load data in instructors tables 
```
load data in instructors with "../resources/SQL/instructors.csv" generate primaryKeyValues provided headers;
```

- Remove entries from students 
```
remove from table students provided sid >= 11;
```

- Remove entries from instructors
```
remove from table instructors provided iid >= 17;
```

- Show student name, numberOfCourses they are taking, their school from students table
```
find ["name", "numberOfCourses", "school"] from students;
find ["name", "numberOfCourses"] from students provided school = "Viterbi School of Engineering";
```

- Show instructors name, their monthlyIncome, their school from students table
```
find ["name", "monthlyIncome", "school"] from instructors;
find ["name", "monthlyIncome"] from instructors provided department = "Computer Science";
```

- Order by 
```
find all from students sorting name ASC;
find all from students sorting name DESC; 
```

- Group By and aggregation
```
find ["SUM(numberOfCourses)"] from students cluster by school;
find ["MAX(numberOfCourses)"] from students cluster by school sorting MAX(numberOfCourses) ASC;
find ["SUM(monthlyIncome)"] from instructors cluster by school;
find ["MIN(monthlyIncome)", "AVG(monthlyIncome)"] from instructors cluster by department sorting department ASC;
```

- Join
```
merge (find ["sid", "name", "department"] from students) as s with (find ["name"] from instructors) as t on s.department = t.department;
merge (find ["name","department","numberOfCourses"] from students) as s with (find ["name","department","monthlyIncome","school"] from instructors) as t on s.school = t.school;
```

### NoSQL Operations:
- List Tables
```
list tables; 
```

- Define table for "students"
```
define table students;
```

- Define table for "instructors"
```
define table instructors;
```

- List Tables
```
list tables; 
```

- Entries for the "students" table
```
fill table students with values {"sid":1, "name":"John Smith", "numberOfCourses":3, "department":"Computer Science", "school":"Viterbi School of Engineering"};
fill table students with values {"sid":2, "name":"Emily Johnson", "numberOfCourses":2, "department":"Electrical Engineering", "school":"Viterbi School of Engineering"};
fill table students with values {"sid":3, "name":"Michael Davis", "numberOfCourses":2, "department":"Mechanical Engineering", "school":"Viterbi School of Engineering"};
fill table students with values {"sid":4, "name":"Sophia Miller", "numberOfCourses":5, "department":"Modern World Arts and Design", "school":"Roski School of Art and Design"};
fill table students with values {"sid":5, "name":"Daniel White", "numberOfCourses":5, "department":"Urban Planning", "school":"Sol Price School of Public Policy"};
fill table students with values {"sid":6, "name":"Olivia Brown", "numberOfCourses":4, "department":"Elements of Music", "school":"Thornton School of Music"};
fill table students with values {"sid":7, "name":"Ethan Wilson", "numberOfCourses":7, "department":"Biomedical Engineering", "school":"Keck School of Medicine"};
fill table students with values {"sid":8, "name":"Ava Martinez", "numberOfCourses":3, "department":"Sociology", "school":"Dornsife College of Letters, Arts and Sciences"};
fill table students with values {"sid":9, "name":"Matthew Lee", "numberOfCourses":1, "department":"Business Analytics", "school":"Marshall School of Business"};
fill table students with values {"sid":10, "name":"Emma Taylor", "numberOfCourses":5, "department":"Environmental Engineering", "school":"Viterbi School of Engineering"};
```

- Entries for the "instructors" table
```
fill table instructors with values {"iid":1, "name":"Professor Anderson", "monthlyIncome":43000,"department":"Computer Science", "school":"Viterbi School of Engineering"};
fill table instructors with values {"iid":2, "name":"Professor Rodriguez", "monthlyIncome":43000, "department":"Electrical Engineering", "school":"Viterbi School of Engineering"};
fill table instrufctors with values {"iid":3, "name":"Professor Turner", "monthlyIncome":41000,"department": "Mechanical Engineering", "school":"Viterbi School of Engineering"};
fill table instructors with values {"iid":4, "name":"Professor Carter", "monthlyIncome":23000, "department":"Chemical Engineering", "school":"Viterbi School of Engineering"};
fill table instructors with values {"iid":5, "name":"Professor Bennett", "monthlyIncome":53000, "department":"Civil Engineering", "school":"Viterbi School of Engineering"};
fill table instructors with values {"iid":6, "name":"Professor Davis", "monthlyIncome":25000, "department":"Biomedical Engineering", "school":"Keck School of Medicine"};
fill table instructors with values {"iid":7, "name":"Professor Smith", "monthlyIncome":10000, "department":"Computer Science", "school":"Viterbi School of Engineering"};
fill table instructors with values {"iid":8, "name":"Professor Johnson", "monthlyIncome":25000, "department":"Business Analytics", "school":"Marshall School of Business"};
fill table instructors with values {"iid":9, "name":"Professor White","monthlyIncome":25000, "department":"Industrial and Systems Engineering", "school":"Viterbi School of Engineering"};
fill table instructors with values {"iid":10, "name":"Professor Miller","monthlyIncome":25000, "department":"Environmental Studies", "school":"Viterbi School of Engineering"};
fill table instructors with values {"iid":11, "name":"Professor Sarah", "monthlyIncome":45000, "department":"Elements of Music", "school":"Thornton School of Music"};
fill table instructors with values {"iid":12, "name":"Professor Taylor", "monthlyIncome":85000, "department":"Business Analytics", "school":"Marshall School of Business"};
fill table instructors with values {"iid":13, "name":"Professor Andrew", "monthlyIncome":55000, "department":"Psychology","school": "Dornsife College of Letters, Arts and Sciences"};
fill table instructors with values {"iid":14, "name":"Professor Johnathan", "monthlyIncome":22000,"department": "Sociology", "school":"Dornsife College of Letters, Arts and Sciences"};
fill table instructors with values {"iid":15, "name":"Professor Suruchi", "monthlyIncome":89000, "department":"Computer Science", "school":"Viterbi School of Engineering"};
fill table instructors with values {"iid":16, "name":"Professor Rahila", "monthlyIncome":22000, "department":"Urban Planning","school": "Sol Price School of Public Policy"};
```

- Show all the entries in "students" table
```
find all from students;
```

- Show all entries in "instructors" table
```
find all from instructors;
```

- Show student name, numberOfCourses they are taking, their school from students table
```
find ["name", "numberOfCourses", "school"] from students;
find ["name", "numberOfCourses"] from students provided school = "Viterbi School of Engineering";
```

- Load data in students tables 
```
load data in students with "../resources/NoSQL/students.csv" generate primaryKeyValues provided headers;
```

- Load data in instructors tables 
```
load data in instructors with "../resources/NoSQL/instructors.csv" generate primaryKeyValues provided headers;
```

- Remove entries from students 
```
remove from table students provided sid >= 11;
```

- Remove entries from instructors
```
remove from table instructors provided iid >= 17;
```

- Show instructors name, their monthlyIncome, their school from students table
```
find ["name", "monthlyIncome", "school"] from instructors;
find ["name", "monthlyIncome"] from instructors provided department = "Computer Science";
```

- Order by 
```
find all from students sorting name ASC;
find all from students sorting name DESC; 
```

- Group By and aggregation
```
find ["SUM(numberOfCourses)"] from students cluster by school;
find ["MAX(numberOfCourses)"] from students cluster by school sorting MAX(numberOfCourses) ASC;
find ["SUM(monthlyIncome)"] from instructors cluster by school;
find ["MIN(monthlyIncome)", "AVG(monthlyIncome)"] from instructors cluster by department sorting department ASC;
```

- Join
```
merge (find ["sid", "name", "department"] from students) with (find ["name", "department"] from instructors) on department = department;
merge (find ["name","department","numberOfCourses", "school"] from students) with (find ["name","department","monthlyIncome","school"] from instructors) on school = school;
```

## SQL Commands
Please make sure to have the resources folder downloaded before running the following commands: 

#### Define Table Operation
- Define Table standings: (Use this table since this has 639 entries with 16 columns)
  ```
  define table standings with [["idx","integer","primaryKey"],["team", "string"], ["team_name", "string"], ["year", "integer"],["wins", "integer"],["loss", "integer"],["points_for", "integer"],["points_against", "integer"],["points_differential", "integer"],["margin_of_victory", "float"],["strength_of_schedule", "float"],["simple_rating", "float"],["offensive_ranking", "float"],["defensive_ranking", "float"], ["playoffs", "string"],["sb_winner", "string"]];
  ```
- Define Table attendance: (Contains 10000+ entries with 8 columns): If you are using this table, kindly wait for some time, depending on the OS, the IO operations would take longer
  ```
  define table attendance with [["idx","integer","primaryKey"], ["team", "string"], ["team_name", "string"], ["year", "integer"], ["total", "string"], ["home", "string"], ["away", "string"], ["week", "string"], ["weekly_attendance", "string"]];
  ```
- Define Table games: (Contains 5000+ entries with 20+ columns): If you are using this table, kindly wait for some time, depending on the OS, the IO operations would take longer
  ```
  define table games with [["idx", "integer","primaryKey"], ["year","integer"], ["week","string"], ["home_team","string"],["away_team","string"], ["winner","string"], ["tie","string"], ["day","string"], ["date","string"], ["time", "string"], ["pts_win","integer"],["pts_loss","integer"], ["yds_win","integer"], ["turnover_win","integer"], ["yds_loss","integer"], ["turnover_loss","integer"], ["home_team_name","string"],["home_team_city","string"], ["away_team_name","string"], ["away_team_city","string"]];
  ```
- Define Table spotify_songs; (Contains 30000+ entries with 24+ columns): If you are using this table, kindly wait for some time, depending on the OS, the IO operations would take longer
  ```
  define table spotify_songs with [["id","integer","primaryKey"],["track_id","string"],["track_name","string"], ["track_artist","string"], ["track_popularity","double"], ["track_album_id","string"],["track_album_name","string"], ["track_album_release_date","string"],["playlist_name","string"], ["playlist_id","string"],["playlist_genre","string"],["playlist_subgenre","string"],["danceability","double"],["energy","double"],["key","double"],["loudness","double"],["mode","integer"],["speechiness","double"],["accousticness","double"],["instrumentalness","double"],["liveness","double"],["valence","double"],["tempo","double"],["duration_ms","double"]];
  ```

#### Insert Bulk CSV
- Load all the data from the CSVs to the table:
  ```
  load data in standings with "../resources/SQL/standings.csv" generate primaryKeyValues provided headers;
  load data in attendance with "../resources/SQL/attendance.csv" generate primaryKeyValues provided headers;
  load data in games with "../resources/SQL/games.csv" generate primaryKeyValues provided headers;
  load data in spotify_songs with "../resources/SQL/spotify_songs.csv" generate primaryKeyValues provided headers;
  ```

#### Find Operation
- Search for all the entries in the database:
  ```
  find all from standings; // <1MB file
  find all from attendance; // <1MB file
  find all from games; // <1MB file
  find all from spotify_songs; //About 8MB file.
  ```
  
- Search for particular entries in the database using "provided" (where) clause:
  ```
  find all from standings provided idx >= 30;
  find all from games provided idx > 300;
  find all from attendance provided team_name = "Vikings";
  find all from spotify_songs provided track_artist = "Ed Sheeran";
  ```
  
- Project particular entries in the database using "provided" (where) clause:
  ```
  find ["team", "team_name", "wins", "year"] from standings provided sb_winner = "Won Superbowl";
  find ["home_team", "tie", "pts_win", "pts_loss"] from games provided day = "Sat";
  find ["track_id", "track_name", "danceability"] from spotify_songs provided track_artist = "Ed Sheeran";
  find ["team", "wins", "loss"] from standings provided team = "India";
  ```

#### Fill Operation
- Fill (insert) data into the tables
  ```
  fill table standings with values [5000, "India", "CSK", 2022, 56, 12, 31, 52, 12, 6.3, 5.2, 5.9, 5.1, 7.3, 8.1, "MII"];
  ```

#### Edit Operation
- Edit (update) existing data in the table
  ```
  edit table standings with values team_name = 'RCB', year = 2023, wins = 0 provided idx = 5000;
  ```

#### Remove Operation
- remove (delete) existing entry in the table
  ```
  remove from table standings provided idx = 5000;
  ```

#### Cluster On and aggregation Operation 
- Group By And Aggregation Operation:
  ```
  find ["CNT(team_name)", "SUM(wins)", "SUM(loss)"] from standings cluster on team;
  find ["SUM(dancability)", "CNT(track_name)", "AVG(dancability)", "MIN(dancability)", "MAX(dancability)", "MIN(energy)", "MAX(energy)"] from spotify_songs provided track_artist = "Ed Sheeran" cluster by track_name;
  ```

#### Sorting Operation 
- Group By And Aggregation Operation:
  ```
  find ["team_name", "team"] from standings sorting team_name DESC;
  find ["track_id", "track_name", "track_artist"] from spotify_songs provided track_artist = "Ed Sheeran" sorting track_name ASC;
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
- Load all the data from the JSONs to the table:
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

## Future Scope: 
#### SQL:
1. Use pointers in BTree representation, to include NULL values.
2. Implement functionality for the where clauses such as: IN, LIKE, AND, OR, Nested Queries.
3. Use Hash Partitioning or Sort-Merge Join algorithm for Joining two or more tables.

#### NoSQL:
1. Implement resizing of the hash when entries are deleted.
2. Implement functionalities for the where clauses such as: LIKE, AND, OR, Range Queries(<,>,<=,>=) and Nested Queries.
3. Prettify the JSON Output that is printed on the terminal.

#### CLI:
1. Add auto-complete functionality on hitting tab on the Keyboard.
2. Enable nested queries execution.
3. Create "Views" and save the intermediate table result.
