/*
Provides an API interface for python to call functions to handle relational database operations
*/

#include "table.h"
#include "pages.h"
#include "attributes.h"

void loadTableMeta(string TableName, unordered_map<string, pair<int, string> >& attrib_index_map) {
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

                attrib_index_map[data[0]] = {idx++, data[1]};
            }
        }

        file.close();
    }
}

int main(int argc, const char **argv) {
    if(argc<2)
        return -1;

    string command = string (argv[1]);
    if(command == "load_data")
    {
        // RelationalAPI.o load_table <table_name> <csv File Location>
        string table_name = string (argv[2]);
        string file_name = string (argv[3]);
        bool hasPrimaryKey = string(argv[4]) == "true";
        bool hasHeader = string(argv[5]) == "true";
        if(insert_entry_in_table(table_name, file_name, hasPrimaryKey, hasHeader))
            return 0;
        else
            return -1;
    }
    else if(command == "define_table")
    {
        // RelationalAPI.o define_table <table_name> <attributes_name, attribute_type, isPrimaryKey> ....
        string table_name = string (argv[2]);
        vector<vector<string>> attributes;
        for(int i=3;i<argc;i+=3)
        {
            attributes.push_back({string(argv[i]), string(argv[i+1]), string(argv[i+2])});
        }

        if(create_table(table_name, attributes))
            return 0;
        else
            return -1;
    }
    else if(command == "fill_table")
    {
        // RelationalAPI.o fill_table <table_name> <value_for_col1> <value_for_col2> <value_for_col3>....
        string table_name = string (argv[2]);
        unordered_map<string, pair<int, string> > attrib_index_map;
        loadTableMeta(table_name, attrib_index_map);
        if (attrib_index_map.size() == 0)
            return -1;

        unordered_map<int, string> index_type_map;
        for(auto& e: attrib_index_map) {
            index_type_map[e.second.first] = e.second.second;
        }

        vector<int> values_int(attrib_index_map.size(), 0);
        vector<float> values_float(attrib_index_map.size(), 0);
        vector<string> values_string(attrib_index_map.size(), "");
        vector<void*> input(attrib_index_map.size(), nullptr);

        for(int i = 3;i < argc; i++)
        {
            if(index_type_map[i-3] == "string")
            {
                values_string[i-3] = string(argv[i]);
                input[i-3] = (void*)&values_string[i-3];
            }
            else if(index_type_map[i-3] == "float")
            {
                values_float[i-3] = stof(string(argv[i]));
                input[i-3] = (void*)&values_float[i-3];
            }
            else if(index_type_map[i-3] == "integer")
            {
                values_int[i-3] = stoi(string(argv[i]));
                input[i-3] = (void*)&values_int[i-3];
            }
        }

        if(insert_entry_in_table(table_name, input))
            return 0;
        else
            return -1;
    }
    else if(command == "edit_table")
    {
        // RelationalAPI.o edit_table <table_name> <set_value_pair_count> <set_col1> <value_for_col1> <set_col2> <value_for_col2> ... <where_col> <operation> <where_value>
        string table_name = string (argv[2]);

        unordered_map<string, pair<int, string> > attrib_index_map;
        loadTableMeta(table_name, attrib_index_map);
        if (attrib_index_map.size() == 0)
            return -1;

        vector<void*> whereClause;
        vector<void*> setClause;

        int setCount = stoi(string(argv[3]));

        vector<string> set_col_names(setCount, "");

        vector<string> set_val_string(setCount, "");
        vector<int> set_val_int(setCount, 0);
        vector<float> set_val_float(setCount, 0);

        int j = 4;
        for(int i=0; i<setCount; i++)
        {
            string col_name = string(argv[j]);
            set_col_names[i] = col_name;
            setClause.push_back((void*)&set_col_names[i]);
            if(attrib_index_map[col_name].second == "integer")
            {
                set_val_int[i] = stoi(string(argv[j+1]));
                setClause.push_back((void*)&set_val_int[i]);
            }
            else if(attrib_index_map[col_name].second == "string")
            {
                set_val_string[i] = string(argv[j+1]);
                setClause.push_back((void*)&set_val_string[i]);
            }
            else if(attrib_index_map[col_name].second == "float")
            {
                set_val_float[i] = stof(string(argv[j+1]));
                setClause.push_back((void*)&set_val_float[i]);
            }
            else
            {
                cout << "Invalid edit column name: "<< col_name << endl;
                return -1;
            }

            j+=2;
        }

        string where_col = string(argv[j]);
        string where_ops = string(argv[j+1]);

        whereClause.push_back((void*)&where_col);
        whereClause.push_back((void*)&where_ops);

        int where_val_int;
        float where_val_float;
        string where_val_str;

        if(attrib_index_map[where_col].second == "integer")
        {
            where_val_int = stoi(string(argv[j+2]));
            whereClause.push_back((void*)&where_val_int);
        }
        else if(attrib_index_map[where_col].second == "string")
        {
            where_val_str = string(argv[j+2]);
            whereClause.push_back((void*)&where_val_str);
        }
        else if(attrib_index_map[where_col].second == "float")
        {
            where_val_float = stof(string(argv[j+2]));
            whereClause.push_back((void*)&where_val_float);
        }
        else
        {
            cout << "Invalid provided that column name: " << where_col << endl;
            return -1;
        }

        if(update_entry_in_table(table_name, whereClause, setClause))
            return 0;
        else
            return -1;
    }
    else if(command == "remove_element")
    {
        // RelationalAPI.o remove_element <table_name> <where_col> <operation> <where_value>
        string table_name = string (argv[2]);

        unordered_map<string, pair<int, string> > attrib_index_map;
        loadTableMeta(table_name, attrib_index_map);
        if (attrib_index_map.size() == 0)
            return -1;

        vector<void*> whereClause;

        string where_col = string(argv[3]);
        string where_ops = string(argv[4]);

        whereClause.push_back((void*)&where_col);
        whereClause.push_back((void*)&where_ops);

        int where_val_int;
        float where_val_float;
        string where_val_str;

        if(attrib_index_map[where_col].second == "integer")
        {
            where_val_int = stoi(string(argv[5]));
            whereClause.push_back((void*)&where_val_int);
        }
        else if(attrib_index_map[where_col].second == "string")
        {
            where_val_str = string(argv[5]);
            whereClause.push_back((void*)&where_val_str);
        }
        else if(attrib_index_map[where_col].second == "float")
        {
            where_val_float = stof(string(argv[5]));
            whereClause.push_back((void*)&where_val_float);
        }
        else
        {
            cout << "Invalid provided that column name: " << where_col << endl;
            return -1;
        }

        if(delete_entry_in_table(table_name, whereClause))
            return 0;
        else
            return -1;

    }
    else if(command == "find_element")
    {
        int idx = 2;
        string table_name = string(argv[idx++]);
        unordered_map<string, pair<int, string> > attrib_index_map;
        loadTableMeta(table_name, attrib_index_map);
        if (attrib_index_map.size() == 0)
            return -1;

        int projectedNum = stoi(string(argv[idx++]));
        vector<string> projectedCols;
        for (int i = 0; i < projectedNum; i++)
        {
            projectedCols.push_back(string(argv[idx++]));
        }
        string whereCol = string(argv[idx++]);
        string whereOps = string(argv[idx++]);
        string whereVal_str = string(argv[idx++]);
        int whereVal_int;
        float whereVal_float;

        string sortCol = string(argv[idx++]);
        string sortOrder = string(argv[idx++]);

        string clusterCol = string(argv[idx++]);

        vector<void*> whereClause;
        pair<string, string> sortClause;

        if (whereCol != "")
        {
            whereClause.push_back((void*)&whereCol);
            whereClause.push_back((void*)&whereOps);
            if (attrib_index_map[whereCol].second == "integer")
            {
                whereVal_int = stoi(whereVal_str);
                whereClause.push_back((void*)&whereVal_int);
            }
            else if (attrib_index_map[whereCol].second == "string")
            {
                whereClause.push_back((void*)&whereVal_str);
            }
            else if (attrib_index_map[whereCol].second == "float")
            {
                whereVal_float = stof(whereVal_str);
                whereClause.push_back((void*)&whereVal_float);
            }
            else
            {
                cout << "Invalid provided that column name: " << whereCol << endl;
                return -1;
            }
        }

        sortClause.first = sortCol;
        sortClause.second = sortOrder;

        if (find_in_table(table_name, projectedCols, whereClause, sortClause, clusterCol))
            return 0;
        else
            return -1;
    }
    else if (command == "merge_tables")
    {
        int idx = 2;
        string table_name1 = string(argv[idx++]);
        unordered_map<string, pair<int, string> > attrib_index_map1;
        loadTableMeta(table_name1, attrib_index_map1);
        if (attrib_index_map1.size() == 0)
            return -1;

        int projectedNum1 = stoi(string(argv[idx++]));
        vector<string> projectedCols1;
        for (int i = 0; i < projectedNum1; i++)
        {
            projectedCols1.push_back(string(argv[idx++]));
        }
        string whereCol1 = string(argv[idx++]);
        string whereOps1 = string(argv[idx++]);
        string whereVal_str1 = string(argv[idx++]);
        int whereVal_int1;
        float whereVal_float1;

        string sortCol1 = string(argv[idx++]);
        string sortOrder1 = string(argv[idx++]);

        string clusterCol1 = string(argv[idx++]);

        vector<void*> whereClause1;
        pair<string, string> sortClause1;

        if (whereCol1 != "")
        {
            whereClause1.push_back((void*)&whereCol1);
            whereClause1.push_back((void*)&whereOps1);
            if (attrib_index_map1[whereCol1].second == "integer")
            {
                whereVal_int1 = stoi(whereVal_str1);
                whereClause1.push_back((void*)&whereVal_int1);
            }
            else if (attrib_index_map1[whereCol1].second == "string")
            {
                whereClause1.push_back((void*)&whereVal_str1);
            }
            else if (attrib_index_map1[whereCol1].second == "float")
            {
                whereVal_float1 = stof(whereVal_str1);
                whereClause1.push_back((void*)&whereVal_float1);
            }
            else
            {
                cout << "Invalid provided that column name: " << whereCol1 << endl;
                return -1;
            }
        }

        sortClause1.first = sortCol1;
        sortClause1.second = sortOrder1;

        string table_identifier1 = string(argv[idx++]);

        string table_name2 = string(argv[idx++]);
        unordered_map<string, pair<int, string> > attrib_index_map2;
        loadTableMeta(table_name2, attrib_index_map2);
        if (attrib_index_map2.size() == 0)
            return -1;

        int projectedNum2 = stoi(string(argv[idx++]));
        vector<string> projectedCols2;
        for (int i = 0; i < projectedNum2; i++)
        {
            projectedCols2.push_back(string(argv[idx++]));
        }
        string whereCol2 = string(argv[idx++]);
        string whereOps2 = string(argv[idx++]);
        string whereVal_str2 = string(argv[idx++]);
        int whereVal_int2;
        float whereVal_float2;

        string sortCol2 = string(argv[idx++]);
        string sortOrder2 = string(argv[idx++]);

        string clusterCol2 = string(argv[idx++]);

        vector<void*> whereClause2;
        pair<string, string> sortClause2;

        if (whereCol2 != "")
        {
            whereClause2.push_back((void*)&whereCol2);
            whereClause2.push_back((void*)&whereOps2);
            if (attrib_index_map2[whereCol2].second == "integer")
            {
                whereVal_int2 = stoi(whereVal_str2);
                whereClause2.push_back((void*)&whereVal_int2);
            }
            else if (attrib_index_map2[whereCol2].second == "string")
            {
                whereClause2.push_back((void*)&whereVal_str2);
            }
            else if (attrib_index_map2[whereCol2].second == "float")
            {
                whereVal_float2 = stof(whereVal_str2);
                whereClause2.push_back((void*)&whereVal_float2);
            }
            else
            {
                cout << "Invalid provided that column name: " << whereCol2 << endl;
                return -1;
            }
        }

        sortClause2.first = sortCol2;
        sortClause2.second = sortOrder2;

        string table_identifier2 = string(argv[idx++]);

        string table1_on_attrib = string(argv[idx++]);
        string on_ops = string(argv[idx++]);
        string table2_on_attrib = string(argv[idx++]);

        string final_sorting_name = string(argv[idx++]);
        string final_sorting_order = string(argv[idx++]);

        vector<string> onClause = { table1_on_attrib , on_ops ,table2_on_attrib };

        if (join_in_table(table_name1, projectedCols1, whereClause1, sortClause1, clusterCol1,
                          table_name2, projectedCols2, whereClause2, sortClause2, clusterCol2,
                          table_identifier1, table_identifier2, onClause, { final_sorting_name , final_sorting_order }))
            return 0;
        else
            return -1;
    }
}