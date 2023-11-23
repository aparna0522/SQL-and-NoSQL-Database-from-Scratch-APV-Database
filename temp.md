NoSQL
Load Data (Load):
load data country-by-continent.json;
load data country-by-abbreviation.json;
load data country-by-currency-name.json;
load data country-by-currency-code.json;

Define table (Create):
define table solar_system;
define table iris;

Fill table (Insert):
Fill table solar_system with values {"planet": "earth", "moons": 1, "color": "blue", "day_hr": 24};
Fill table solar_system with values {"planet": "mars", "moons": 0, "color": "red", "day_hr": 25};
fill table country-by-abbreviation with values {"country": "Verdansk", "abbreviation": "VDK", "id": 246};
fill table country-by-abbreviation with values {"country": "Wakanda", "abbreviation": "WNK", "id": 247};
fill table country-by-abbreviation with values {"country": "Urzikstan", "abbreviation": "URS", "id": 248};


Edit table (Update):
edit table country-by-abbreviation with values abbreviation = WND provided country = Wakanda;


Remove table (Delete):
remove from table country-by-abbreviation provided id in [246, 247];
remove from table country-by-abbreviation provided id = 248;


Find table (Select):
find ["continent", "country"] from country-by-continent provided continent in ["Asia", "Africa"] sorting continent asc;
find ["continent", "country"] from country-by-continent provided continent in ["Asia", "Africa"] sorting continent desc;
find all from country-by-continent provided continent = Antarctica sorting country desc;
find ["CNT(country)", "SUM(population)"] from country-by-continent cluster on continent sorting continent asc;
find ["CNT(country)", "SUM(population)"] from country-by-continent cluster on continent sorting CNT(country) desc;
find ["CNT(country)", "SUM(population)"] from country-by-continent cluster on continent sorting SUM(population) desc;

Merge table (Join):
merge (find all from country-by-currency-code) with (find all from country-by-currency-name) on country = country;
merge (find ["country", "continent"] from country-by-continent provided continent = Asia) with (find ["country", "abbreviation"] from country-by-abbreviation) on country = country sorting s.continent asc;

