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
  ```
  define table students with [["sid", "integer", "primaryKey"],["name", "string"], ["address", "string"]];
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
  ```

define table standings_1 with [["idx", "integer", "PrimaryKey"], ["team", "string"], ["team_name", "string"], ["year", "integer"], ["wins", "integer"], ["loss", "integer"], ["points_for", "integer"], ["points_against", "integer"], ["points_differential", "integer"], ["margin_of_victory", "float"], ["strength_of_schedule", "float"], ["simple_rating", "float"], ["offensive_ranking", "float"], ["defensive_ranking", "float"], ["play_offs", "string"], ["sb_winner", "string"]];

load data in standings_1 with "../standings.csv";

fill table standings with values [5000, "India", "CSK", 2022, 56, 12, 31, 52, 12, 6.3, 5.2, 5.9, 5.1, 7.3, 8.1, "MII"];
find ["team", "wins", "loss"] from standings provided team = "India";

edit table standings with values team_name = 'RCB', year = 2023, wins = 0 provided idx = 5000;

remove from table standings provided idx = 5000

find all from standings;
find ["team", "team_name", "wins", "year"] from standings;
find ["team", "team_name", "wins", "year"] from standings provided wins = 10;
find ["CNT(team_name)", "SUM(wins)", "SUM(loss)"] from standings provided wins > 10 cluster on team sorting SUM(wins) DESC;
   
merge (find ["team", "team_name"] from standings) as t with (find ["team", "wins", "loss"] from standings) as s on s.team = t.team;
merge (find ["team", "team_name"] from standings) as t with (find ["team", "wins", "loss"] from standings) as s on s.team = t.team sorting t.team DESC;
