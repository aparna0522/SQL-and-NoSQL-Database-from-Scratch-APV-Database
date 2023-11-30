/*
Compile this .cpp file to run relational-DB in menu mode
*/

#include "../attributes.h"
#include "../pages.h"
#include "../table.h"

string table_name = "";

void loadTableMeta(
   string TableName,
   unordered_map<string, pair<int, string> >& attrib_index_map) {
 ifstream file("databases/" + TableName + "/meta.txt");
 if (file.is_open()) {
   string line;
   int idx = 0;
   while (getline(file, line)) {
     if (line.substr(0, min(12, (int)line.size())) == "PrimaryKey: ") {
       continue;
     } else {
       string word = "";
       stringstream s(line);
       vector<string> data;

       while (getline(s, word, char(170))) data.push_back(word);

       attrib_index_map[data[0]] = {idx, data[1]};
     }
   }

   file.close();
 }
}

// Table operations
bool performTableCreation() {
 cout << "\nCreate Table\n";
 cout << "\n---------------------------------\n";

 string tableNameInsert;
 cout << "Enter table name: ";
 getline(cin, tableNameInsert);
 vector<vector<string> > attributes;

 while (true) {
   vector<string> att(3, "");
   cout << "Enter attribute name: ";
   getline(cin, att[0]);

   if (att[0] == "") break;

   cout << "Enter attribute type: ";
   getline(cin, att[1]);

   cout << "Enter attribute isPrimary: ";
   getline(cin, att[2]);

   attributes.push_back(att);
 }

 if (attributes.size() == 0) return false;

 if (create_table(tableNameInsert, attributes)) table_name = tableNameInsert;

 return true;
}

bool performTableDeletion() {
 if (delete_table(table_name))
   return true;
 else
   return false;
}

// Entries operations

bool performInsertions(string& TableName) {
 unordered_map<string, pair<int, string> > attrib_index_map;
 loadTableMeta(TableName, attrib_index_map);

 vector<int> values_int(attrib_index_map.size(), 0);
 vector<float> values_float(attrib_index_map.size(), 0);
 vector<double> values_double(attrib_index_map.size(), 0);
 vector<string> values_string(attrib_index_map.size(), "");
 vector<void*> input(attrib_index_map.size(), nullptr);

 cout << "\nInsertion\n";
 cout << "\n---------------------------------\n";

 for (auto& attrib : attrib_index_map) {
   int index = attrib.second.first;
   cout << attrib.first << ": ";
   if (attrib.second.second == "integer") {
     cin >> values_int[index];
     cin.ignore();
     input[index] = (void*)&values_int[index];
   } else if (attrib.second.second == "string") {
     getline(cin, values_string[index]);
     input[index] = (void*)&values_string[index];
   } else if (attrib.second.second == "float") {
     cin >> values_float[index];
     input[index] = (void*)&values_float[index];
   } else if (attrib.second.second == "double") {
     cin >> values_double[index];
     input[index] = (void*)&values_double[index];
   }
 }

 if (insert_entry_in_table(TableName, input))
   cout << "Inserted the element\n";
 else
   return false;
 cout << "\n---------------------------------\n";

 return true;
}

bool performDeletions(string& table_name) {
 unordered_map<string, pair<int, string> > attrib_index_map;
 loadTableMeta(table_name, attrib_index_map);

 cout << "\nDeletion\n";
 cout << "\n---------------------------------\n";

 string where_attrib;
 int where_value_i;
 float where_value_f;
 double where_value_d;
 string where_value_s;
 string operation = "";

 vector<void*> where_clause;

 cout << "Attribute you want to delete: ";
 getline(cin, where_attrib);

 if (where_attrib == "")
   return true;

 else if (where_attrib != "*") {
   where_clause.push_back((void*)&where_attrib);

   cout << "operation: ";
   getline(cin, operation);

   where_clause.push_back((void*)&operation);

   void* where_value = nullptr;
   cout << "Where " << where_attrib << " " << operation << " ";
   if (attrib_index_map[where_attrib].second == "integer") {
     cin >> where_value_i;
     cin.ignore();
     where_value = (void*)&where_value_i;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "string") {
     getline(cin, where_value_s);
     where_value = (void*)&where_value_s;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "float") {
     cin >> where_value_f;
     cin.ignore();
     where_value = (void*)&where_value_f;
     where_clause.push_back(where_value);
   }
   else if (attrib_index_map[where_attrib].second == "double") {
     cin >> where_value_d;
     cin.ignore();
     where_value = (void*)&where_value_d;
     where_clause.push_back(where_value);
   }
 }
 delete_entry_in_table(table_name, where_clause);

 cout << "\n---------------------------------\n";

 return true;
}

bool performUpdations(string& table_name) {
 unordered_map<string, pair<int, string> > attrib_index_map;
 loadTableMeta(table_name, attrib_index_map);

 cout << "\nUpdation\n";
 cout << "\n---------------------------------\n";

 // Where Clause
 string where_attrib;
 int where_value_i;
 string where_value_s;
 string operation = "";

 // Set Clause
 string set_attrib;
 int set_value_i;
 float where_value_f;
 double where_value_d;
 string set_value_s;
 vector<void*> where_clause;

 cout << "Attribute Name you want to filter against: ";
 getline(cin, where_attrib);

 if (where_attrib == "")
   return true;

 else if (where_attrib != "*") {
   where_clause.push_back((void*)&where_attrib);

   cout << "operation: ";
   getline(cin, operation);

   where_clause.push_back((void*)&operation);

   void* where_value = nullptr;
   cout << "Where " << where_attrib << " " << operation << " ";
   if (attrib_index_map[where_attrib].second == "integer") {
     cin >> where_value_i;
     cin.ignore();
     where_value = (void*)&where_value_i;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "string") {
     getline(cin, where_value_s);
     where_value = (void*)&where_value_s;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "float") {
     cin >> where_value_f;
     cin.ignore();
     where_value = (void*)&where_value_f;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "double") {
     cin >> where_value_d;
     cin.ignore();
     where_value = (void*)&where_value_d;
     where_clause.push_back(where_value);
   }
 }

 void* where_value = nullptr;
 void* set_value = nullptr;
 cout << "Attribute Name you want to set:";
 getline(cin, set_attrib);

 cout << "Set " << set_attrib << " = ";
 if (set_attrib == "sid") {
   cin >> set_value_i;
   cin.ignore();
   set_value = (void*)&set_value_i;
 } else {
   getline(cin, set_value_s);
   set_value = (void*)&set_value_s;
 }

 update_entry_in_table(table_name, where_clause,
                       {(void*)&set_attrib, set_value});

 cout << "\n---------------------------------\n";

 return true;
}

bool performSearch(string& table_name) {
 unordered_map<string, pair<int, string> > attrib_index_map;
 loadTableMeta(table_name, attrib_index_map);

 //{"sid", "<", (void*)7}, {"sid", "name"});
 cout << "\nSearch\n";
 cout << "\n---------------------------------\n";

 // Where Clause
 string where_attrib;
 int where_value_i;
 float where_value_f;
 double where_value_d;
 string where_value_s;
 string operation = "";

 vector<void*> where_clause;

 cout << "Attribute Name you want to filter against: ";
 getline(cin, where_attrib);

 if (where_attrib == "")
   return true;

 else if (where_attrib != "*") {
   where_clause.push_back((void*)&where_attrib);
   cout << "operation: ";
   getline(cin, operation);

   where_clause.push_back((void*)&operation);

   void* where_value = nullptr;
   cout << "Where " << where_attrib << " " << operation << " ";
   if (attrib_index_map[where_attrib].second == "integer") {
     cin >> where_value_i;
     cin.ignore();
     where_value = (void*)&where_value_i;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "string") {
     getline(cin, where_value_s);
     where_value = (void*)&where_value_s;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "float") {
     cin >> where_value_f;
     cin.ignore();
     where_value = (void*)&where_value_f;
     where_clause.push_back(where_value);
   }
   else if (attrib_index_map[where_attrib].second == "double") {
     cin >> where_value_d;
     cin.ignore();
     where_value = (void*)&where_value_d;
     where_clause.push_back(where_value);
   }
 }
 // Projection Fields
 vector<string> projectFileds;
 while (true) {
   string fieldName;
   cout << "Attribute Name you want to project: ";
   getline(cin, fieldName);
   if (fieldName == "")
     break;
   else
     projectFileds.push_back(fieldName);
 }

 search_in_table(table_name, where_clause, projectFileds);
 cout << "\n---------------------------------\n";
}

bool performOrderedSearch(string& table_name) {
 unordered_map<string, pair<int, string> > attrib_index_map;
 loadTableMeta(table_name, attrib_index_map);

 cout << "Ordered Search\n";
 cout << "\n---------------------------------\n";

 // Where Clause
 string where_attrib;
 int where_value_i;
 float where_value_f;
 double where_value_d;
 string where_value_s;
 string operation = "";

 vector<void*> where_clause;

 string attrib_to_sort;
 string sort_order;

 cout << "Attribute Name you want to filter against: ";
 getline(cin, where_attrib);

 if (where_attrib == "")
   return true;
 else if (where_attrib != "*") {
   where_clause.push_back((void*)&where_attrib);
   cout << "operation: ";
   getline(cin, operation);

   where_clause.push_back((void*)&operation);

   void* where_value = nullptr;
   cout << "Where " << where_attrib << " " << operation << " ";
   if (attrib_index_map[where_attrib].second == "integer") {
     cin >> where_value_i;
     cin.ignore();
     where_value = (void*)&where_value_i;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "string") {
     getline(cin, where_value_s);
     where_value = (void*)&where_value_s;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "float") {
     cin >> where_value_f;
     cin.ignore();
     where_value = (void*)&where_value_f;
     where_clause.push_back(where_value);
   }
   else if (attrib_index_map[where_attrib].second == "double") {
     cin >> where_value_d;
     cin.ignore();
     where_value = (void*)&where_value_d;
     where_clause.push_back(where_value);
   }
 }

 // Projection Fields
 vector<string> projectFileds;
 while (true) {
   string fieldName;
   cout << "Attribute Name you want to project: ";
   getline(cin, fieldName);
   if (fieldName == "")
     break;
   else
     projectFileds.push_back(fieldName);
 }

 cout << "Attribute Name you want to sort with: ";
 getline(cin, attrib_to_sort);

 cout << "Attribute sort order (ASC/DESC): ";
 getline(cin, sort_order);

 order_by_in_table(table_name, where_clause, projectFileds,
                   {attrib_to_sort, sort_order});
 cout << "\n---------------------------------\n";
}

bool performJoin() {
 unordered_map<string, pair<int, string> > attrib_index_map[2];
 cout << "Join\n";
 cout << "\n---------------------------------\n";

 string table1;
 cout << "Name of Table1: ";
 getline(cin, table1);
 loadTableMeta(table1, attrib_index_map[0]);

 // Where Clause
 string where_attrib[2];
 int where_value_i[2];
 float where_value_f[2];
 double where_value_d[2];
 string where_value_s[2];
 string operation[2] = {"", ""};

 vector<void*> where_clause[2];

 cout << "Attribute Name you want to filter against: ";
 getline(cin, where_attrib[0]);

 if (where_attrib[0] == "")
   return true;
 else if (where_attrib[0] != "*") {
   where_clause[0].push_back((void*)&where_attrib[0]);

   cout << "operation: ";
   getline(cin, operation[0]);

   where_clause[0].push_back((void*)&operation[0]);

   void* where_value = nullptr;
   cout << "Where " << where_attrib[0] << " " << operation[0] << " ";
   if (attrib_index_map[0][where_attrib[0]].second == "integer") {
     cin >> where_value_i[0];
     cin.ignore();
     where_value = (void*)&where_value_i[0];
     where_clause[0].push_back(where_value);
   } else if (attrib_index_map[0][where_attrib[0]].second == "string") {
     getline(cin, where_value_s[0]);
     where_value = (void*)&where_value_s[0];
     where_clause[0].push_back(where_value);
   } else if (attrib_index_map[0][where_attrib[0]].second == "float") {
     cin >> where_value_f[0];
     cin.ignore();
     where_value = (void*)&where_value_f[0];
     where_clause[0].push_back(where_value);
   }
   else if (attrib_index_map[0][where_attrib[0]].second == "double") {
     cin >> where_value_d[0];
     cin.ignore();
     where_value = (void*)&where_value_d[0];
     where_clause[0].push_back(where_value);
   }
 }

 // Projection Fields
 vector<string> projectFileds[2];
 while (true) {
   string fieldName;
   cout << "Attribute Name you want to project: ";
   getline(cin, fieldName);
   if (fieldName == "")
     break;
   else
     projectFileds[0].push_back(fieldName);
 }

 string table2;
 cout << "Name of Table2: ";
 getline(cin, table2);
 loadTableMeta(table2, attrib_index_map[1]);

 cout << "Attribute Name you want to filter against: ";
 getline(cin, where_attrib[1]);

 if (where_attrib[1] == "")
   return true;
 else if (where_attrib[1] != "*") {
   where_clause[1].push_back((void*)&where_attrib[1]);

   cout << "operation: ";
   getline(cin, operation[1]);

   where_clause[1].push_back((void*)&operation[1]);

   void* where_value = nullptr;
   cout << "Where " << where_attrib[1] << " " << operation[1] << " ";
   if (attrib_index_map[1][where_attrib[1]].second == "integer") {
     cin >> where_value_i[1];
     cin.ignore();
     where_value = (void*)&where_value_i[1];
     where_clause[1].push_back(where_value);
   } else if (attrib_index_map[1][where_attrib[1]].second == "string") {
     getline(cin, where_value_s[1]);
     where_value = (void*)&where_value_s[1];
     where_clause[1].push_back(where_value);
   } else if (attrib_index_map[1][where_attrib[1]].second == "float") {
     cin >> where_value_f[1];
     cin.ignore();
     where_value = (void*)&where_value_f[1];
     where_clause[1].push_back(where_value);
   }
   else if (attrib_index_map[1][where_attrib[1]].second == "double") {
     cin >> where_value_d[1];
     cin.ignore();
     where_value = (void*)&where_value_d[1];
     where_clause[1].push_back(where_value);
   }
 }

 while (true) {
   string fieldName;
   cout << "Attribute Name you want to project: ";
   getline(cin, fieldName);
   if (fieldName == "")
     break;
   else
     projectFileds[1].push_back(fieldName);
 }

 vector<string> OnClause;
 string onClause_str;

 cout << "Perform join on attrib from table1: ";
 getline(cin, onClause_str);
 OnClause.push_back(onClause_str);

 cout << "Perform join on " << onClause_str << " ";
 getline(cin, onClause_str);
 OnClause.push_back(onClause_str);

 cout << "Perform join on " << OnClause[0] << " " << OnClause[1] << " ";
 getline(cin, onClause_str);
 OnClause.push_back(onClause_str);

 join_in_table(table1, where_clause[0], projectFileds[0], table2,
               where_clause[1], projectFileds[1], OnClause);

 cout << "\n---------------------------------\n";
}

bool performGroupingAggregation(string& table_name) {
 unordered_map<string, pair<int, string> > attrib_index_map;
 loadTableMeta(table_name, attrib_index_map);
 cout << "Grouping and Aggregation\n";
 cout << "\n---------------------------------\n";

 // Where Clause
 string where_attrib;
 int where_value_i;
 float where_value_f;
 double where_value_d;
 string where_value_s;
 string operation = "";

 vector<void*> where_clause;

 cout << "Attribute Name you want to filter against: ";
 getline(cin, where_attrib);

 if (where_attrib == "")
   return true;

 else if (where_attrib != "*") {
   where_clause.push_back((void*)&where_attrib);
   cout << "operation: ";
   getline(cin, operation);

   where_clause.push_back((void*)&operation);

   void* where_value = nullptr;
   cout << "Where " << where_attrib << " " << operation << " ";
   if (attrib_index_map[where_attrib].second == "integer") {
     cin >> where_value_i;
     cin.ignore();
     where_value = (void*)&where_value_i;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "string") {
     getline(cin, where_value_s);
     where_value = (void*)&where_value_s;
     where_clause.push_back(where_value);
   } else if (attrib_index_map[where_attrib].second == "float") {
     cin >> where_value_f;
     cin.ignore();
     where_value = (void*)&where_value_f;
     where_clause.push_back(where_value);
   }
   else if (attrib_index_map[where_attrib].second == "double") {
     cin >> where_value_d;
     cin.ignore();
     where_value = (void*)&where_value_d;
     where_clause.push_back(where_value);
   }
 }

 string groupbyname;
 cout << "Please enter the column name by which you want to group by: ";
 getline(cin, groupbyname);
 // Aggregation Fields
 vector<pair<string, string> > aggregateClause;
 while (true) {
   pair<string, string> ag;
   cout << "Attribute Name you want to aggregate: ";
   getline(cin, ag.first);
   if (ag.first == "") break;

   cout << "Aggregation function name: ";
   getline(cin, ag.second);
   aggregateClause.push_back(ag);
 }

 group_and_aggregate_in_table(table_name, where_clause, groupbyname,
                              aggregateClause);
 cout << "\n---------------------------------\n";
}

bool insertBulkCSV(string& table_name) {
 cout << "\nBulk data insertion from CSV\n";
 cout << "\n---------------------------------\n";

 string csvLocation = "";
 cout << "Enter the CSV location: ";
 getline(cin, csvLocation);

 if (insert_entry_in_table(table_name, csvLocation, false, true))
   cout << "Inserted all the elements\n";
 else
   return false;
 cout << "\n---------------------------------\n";

 return true;
}

int main() {
 /*table_name = "students";
 vector<vector<string>> table_attributes = {
         {"sid", "integer", "PK"},
         {"name", "string", ""},
         {"department", "string", ""}
 };
 if (create_table(table_name, table_attributes))
     cout << "Table created successfully!" << endl;
 else
     cerr << "Table name already exists in the database." << endl;

 table_name = "instructors";
 table_attributes.clear();
 table_attributes.insert(table_attributes.begin(), {
         {"iid", "integer", "PK"},
         {"name", "string", ""},
         {"department", "string", ""}
 });
 if (create_table(table_name, table_attributes))
     cout << "Table created successfully!" << endl;
 else
     cerr << "Table name already exists in the database." << endl; */

 while (true) {
   int decision;
   cout << "Enter the operation you want to perform\n\t1.Use "
           "Table\n\t2.Create Table\n\t3.Delete "
           "Table\n\t4.Insertion\n\t5.Updation\n\t6.Search\n\t7.Deletion\n\t8."
           "Ordered search\n\t9.Join\n\t10.Grouping/Aggregation\n\t11.Insert "
           "Bulk CSV\n\nDecision: ";
   cin >> decision;
   cin.ignore();
   switch (decision) {
     case 1:
       cout << "Enter the table name you want to perform operations on: ";
       getline(cin, table_name);
       break;
     case 2:
       performTableCreation();
       break;
     case 3:
       performTableDeletion();
       break;
     case 4:
       performInsertions(table_name);
       break;
     case 5:
       performUpdations(table_name);
       break;
     case 6:
       performSearch(table_name);
       break;
     case 7:
       performDeletions(table_name);
       break;
     case 8:
       performOrderedSearch(table_name);
       break;
     case 9:
       performJoin();
       break;
     case 10:
       performGroupingAggregation(table_name);
       break;
     case 11:
       insertBulkCSV(table_name);
       break;
     default:
       break;
   }
 }

 //    cout << "Table deleted successfully!!" << endl;
 //    cerr << "Failed to delete the table. Table not found!" << endl;

 return 0;
}