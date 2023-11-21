# APV-Database
Repo containing self built Relational Database System as well as NoSQL database, along with a CLI and a natural language for a user to interact with

## SQL Commands - 
When running sql commands use following syntax:

#### Define Table
  ```
  define table standings with [["idx","integer","primaryKey"],["team", "string"], ["team_name", "string"], ["year", "integer"],["wins", "integer"],["loss", "integer"],["points_for", "integer"],["points_against", "integer"],["points_differential", "integer"],["margin_of_victory", "float"],["strength_of_schedule", "float"],["simple_rating", "float"],["offensive_ranking", "float"],["defensive_ranking", "float"], ["playoffs", "string"],["sb_winner", "string"]];
  ```
  ```
  define table attendance with [["idx","integer","primaryKey"], ["team", "string"], ["team_name", "string"], ["year", "integer"], ["total", "string"], ["home", "string"], ["away", "string"], ["week", "string"], ["weekly_attendance", "string"]];
  ```
  ```
  define table games with [["idx", "integer","primaryKey"], ["year","integer"], ["week","string"], ["home_team","string"],["away_team","string"], ["winner","string"], ["tie","string"], ["day","string"], ["date","string"], ["time", "string"], ["pts_win","integer"],["pts_loss","integer"], ["yds_win","integer"], ["turnover_loss","integer"], ["home_team_name","string"],["home_team_city","string"], ["away_team_name","string"], ["away_team_city","string"]];
  ```
#### Insert Bulk CSV
  ```
  load data in standings with "/Users/admin/Desktop/standings.csv" generate primaryKeyValues provided headers;
  load data in attendance with "/Users/admin/Desktop/attendance.csv" generate primaryKeyValues provided headers;
  load data in games with "/Users/admin/Desktop/games.csv" generate primaryKeyValues provided headers;
  ```
#### Find operation
  ```
  find all from standings;
  find all from attendance;
  find all from games;
  ```
  ```
  find all from standings provided idx > 300;
  find all from games provided team = "Vikings"; //team is a wrong column name
  find all from games provided home_team = "Vikings"; //"Vikings" team name does not exist
  find all from games provided home_team = "Los Angeles Rams"; 
  Indianapolis
  find all from attendance provided team = "Vikings";
  ```

