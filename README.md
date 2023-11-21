# APV-Database
Repo containing self built Relational Database System as well as NoSQL database, along with a CLI and a natural language for a user to interact with

## SQL Commands - 
When running sql commands use following syntax:

#### Define Table
  ```
  define table standings with [["idx","integer","primaryKey"],["team", "string"], ["team_name", "string"], ["year", "integer"],["wins", "integer"],["loss", "integer"],["points_for", "integer"],["points_against", "integer"],["points_differential", "integer"],["margin_of_victory", "float"],["strength_of_schedule", "float"],["simple_rating", "float"],["offensive_ranking", "float"],["defensive_ranking", "float"], ["playoffs", "string"],["sb_winner", "string"]];
  ```

#### Insert Bulk CSV
  ```
  load data in standings with <ABSOLUTE_PATH_TO_CSV> generate primaryKey given headers;
  ```

#### Find operation
  ```
  find all from standings;
  ```
