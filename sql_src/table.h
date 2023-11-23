/*
Table operations, creation, deletion
super functions for inserting entries, deleting entries, updating entries, search entries and,
process: join, group & aggregate and order
*/
#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <set>
#include<algorithm>
#include <sstream>
#include <unordered_map>
#include "common.h"
#include "attributes.h"
#include "pages.h"

using namespace std;
using namespace __fs;

int getNewTempFolderIndex()
{
    int start = 0;
    if (!filesystem::is_directory("temp") && !filesystem::exists("temp")) {
        filesystem::create_directory("temp");
        ofstream metaFile("temp/meta.txt");
        metaFile << "Folder_Index: " << start << endl;
        metaFile.close();
    }
    else {
        ifstream readMetaFile("temp/meta.txt");
        string line;
        while (getline(readMetaFile, line)) {
            if (line.substr(0, 14) == "Folder_Index: ")
            {
                start = stoi(line.substr(14, line.size() - 14));
            }
        }
        readMetaFile.close();
    }
    return start;
}

void printCell(string& data, int col_width)
{
    int padding = col_width - data.size() - 1;
    std::cout << string(padding, ' ') << data << "|";
}

void printProjectedValue(string temp_folder_location)
{
    ifstream readMeta(temp_folder_location + "/meta.txt");
    string metaLine;

    unordered_map<int, int> index_width;
    const int truncated_limit = PRINT_ROWS;
    vector<int> locs;

    while (getline(readMeta, metaLine))
    {
        if (metaLine.substr(0, min(12, (int)metaLine.size())) == "PrimaryKey: ")
            continue;

        stringstream ss(metaLine);
        string word;
        vector<string> data;
        while (getline(ss, word, char(170)))
            data.push_back(word);
        index_width[stoi(data[0])] = max(index_width[stoi(data[0])], (int)data[1].size());
        locs.push_back(stoi(data[0]));
    }
    readMeta.close();

    int page = 0;
    int entries = 0;
    while(true)
    {
        ifstream pageFile(temp_folder_location + "/pages_" + to_string(page) + ".txt");
        string line;

        if (pageFile.is_open())
        {
            while (getline(pageFile, line) && entries < truncated_limit) {
                line = line.substr(1, line.size() - 2);
                stringstream ss(line);
                vector<string> data;
                string word;
                while (getline(ss, word, char(170)))
                    data.push_back(word);
                for (int i = 0; i < locs.size(); i++) {
                    index_width[locs[i]] = max(index_width[locs[i]], (int)data[locs[i]].size());
                }
                entries++;
            }
            pageFile.close();
        }
        else
            break;

        page++;
    }

    int table_width = 0;
    int padding = 3;
    for (auto& e : index_width)
        table_width += e.second + padding;
    table_width ++;

    readMeta.open(temp_folder_location + "/meta.txt");
    for (int i = 0; i < locs.size(); i++)
    {
        std::cout<<"+"<<string(index_width[locs[i]] + padding - 1, '-');
    }
    std::cout << "+" << endl << "|";
    while (getline(readMeta, metaLine))
    {
        if (metaLine.substr(0, min(12, (int)metaLine.size())) == "PrimaryKey: ")
            continue;

        stringstream ss(metaLine);
        string word;
        vector<string> data;
        while (getline(ss, word, char(170)))
            data.push_back(word);

        printCell(data[1], index_width[(stoi(data[0]))] + padding);
    }
    std::cout << endl;
    for (int i = 0; i < locs.size(); i++)
    {
        std::cout << "+" << string(index_width[locs[i]] + padding - 1, '-');
    }
    std::cout << "+" << endl;
    readMeta.close();

    entries = 0;
    int i;
    for (i = 0; i < page; i++)
    {
        ifstream primaryKeySearch(temp_folder_location + "/pages_" + to_string(i) + ".txt");
        string line;

        if (primaryKeySearch.is_open())
        {
            while (getline(primaryKeySearch, line) && entries < truncated_limit) {
                std::cout << "|";
                line = line.substr(1, line.size() - 2);
                stringstream ss(line);
                vector<string> data;
                string word;
                while (getline(ss, word, char(170)))
                    data.push_back(word);
                for (int i = 0; i < locs.size(); i++) {
                    printCell(data[locs[i]], index_width[locs[i]] + padding);
                }
                std::cout << endl;
                entries++;
            }
            primaryKeySearch.close();
        }

        if (entries == truncated_limit)
            break;
    }
    for (int i = 0; i < locs.size(); i++)
    {
        std::cout << "+" << string(index_width[locs[i]] + padding - 1, '-');
    }
    std::cout << "+" << endl;
    if (i < page && entries == truncated_limit)
        std::cout << "Truncated the output printing as entries exceeded " << truncated_limit << endl;
    else
        std::cout << entries << " rows in set." << endl;
}

// Function to create a table (folder) and its subdirectories
bool create_table(const string& table_name, const vector<vector<string>>& table_attributes) {
    filesystem::path rootDir = filesystem::current_path();

    if (filesystem::is_directory("databases") || filesystem::exists("databases"))
        filesystem::current_path("databases");
    else
    {
        filesystem::create_directory("databases");
        filesystem::current_path("databases");
    }
    try {
        if (filesystem::is_directory(table_name) || filesystem::exists(table_name))
            throw invalid_argument("Table with this same name already exists");

        // Create the folder structure for the table and navigate inside the folder
        filesystem::create_directory(table_name);
        filesystem::current_path(table_name);

        ofstream metaFile("meta.txt");

        bool isComposite = false;
        string primaryKey = "";

        if (metaFile.is_open()) {
            for (int i = 0; i < table_attributes.size(); i++) {
                metaFile << table_attributes[i][0] << char(170) << table_attributes[i][1] << endl;
                if (table_attributes[i][2] == "PK") {
                    if (primaryKey == "") {
                        primaryKey = table_attributes[i][0];
                    }
                    else {
                        primaryKey += "," + table_attributes[i][0];
                        isComposite = true;
                    }
                }
            }
            metaFile << "PrimaryKey: " << primaryKey;
            metaFile.close();
        }
        else {
            cerr << "Failed writing the data file" << endl;
        }
        // Iterate through all the attributes of the table and create directories and subdirectories.
        // Each subdirectory has a meta.txt file describing the metadata required when performing CRUD Operations.
        for (auto& attribute : table_attributes) {
            string subfolder = attribute[0];

            filesystem::create_directory(subfolder);
            filesystem::current_path(subfolder);

            filesystem::path currentDir = filesystem::current_path();

            ofstream dataFile("data_0.txt");
            if (dataFile.is_open()) {
                for (int i = 0; i < 2 * ORDER - 1; i++) {
                    dataFile << std::endl;
                }
                dataFile.close();
            }
            else {
                cerr << "Failed writing the data file" << endl;
            }

            // Writing meta data to the file meta.txt present in every subdirectory
            ofstream outputFile("meta.txt");
            if (outputFile.is_open()) {
                outputFile << "DataType: " + attribute[1] << std::endl;
                outputFile << "Root: 0" << std::endl;
                outputFile << "Nodes: 0";
                outputFile.close();
            }
            else {
                cerr << "Failed writing the meta data for the attribute meta.txt" << endl;
            }
            filesystem::path newDir = currentDir.parent_path();
            filesystem::current_path(newDir);
        }

        // Create a composite key value create a subfolder (column representing that as primary key)
        if (isComposite)
        {
            string subfolder = primaryKey;

            filesystem::create_directory(subfolder);
            filesystem::current_path(subfolder);

            filesystem::path currentDir = filesystem::current_path();

            ofstream dataFile("data_0.txt");
            if (dataFile.is_open()) {
                for (int i = 0; i < 2 * ORDER - 1; i++) {
                    dataFile << std::endl;
                }
                dataFile.close();
            }
            else {
                cerr << "Failed writing the data file" << endl;
            }

            // Writing meta data to the file meta.txt present in every subdirectory
            ofstream outputFile("meta.txt");
            if (outputFile.is_open()) {
                outputFile << "DataType: string" << std::endl;
                outputFile << "Root: 0" << std::endl;
                outputFile << "Nodes: 0";
                outputFile.close();
            }
            else {
                cerr << "Failed writing the meta data for the attribute meta.txt" << endl;
            }
            filesystem::path newDir = currentDir.parent_path();
            filesystem::current_path(newDir);
        }

        filesystem::current_path(rootDir);
        return true;
    }
    catch (const exception& e) {
        cerr << "Error creating table: " << e.what() << endl;
        filesystem::current_path(rootDir);
        return false;
    }
}

// Function to delete a table (folder) and its subdirectories
bool delete_table(const string& table_name) {
    filesystem::path rootDir = filesystem::current_path();
    if (filesystem::is_directory("databases") || filesystem::exists("databases"))
        filesystem::current_path("databases");
    else
    {
        cerr << "Error deleting the table" << endl;
        filesystem::current_path(rootDir);
        return false;
    }
    try {
        filesystem::remove_all(table_name);
        return true;
    }
    catch (const exception& e) {
        cerr << "Error deleting the table" << e.what() << endl;
        return false;
    }

    filesystem::current_path(rootDir);
}

bool insert_entry_in_table(string table_name, vector<void*> values) {
    table_name = "databases/" + table_name;
    ifstream file(table_name + "/meta.txt");
    // index in the values vector + its type
    vector<int> primaryKeyIndex;
    string primaryKeyName = "";
    string valueInPK = "{";
    string valueInOther = "";
    unordered_map<string, pair<int, string>> attrib_index_map;
    if (file.is_open())
    {
        string line;
        int idx = 0;
        while (getline(file, line)) {
            if (line.substr(0, min(12, (int)line.size())) == "PrimaryKey: ")
            {
                string primaryKey = line.substr(12, line.size() - 12);
                primaryKeyName = primaryKey;
                string word = "";
                vector<string> data;
                stringstream s(primaryKey);

                while (getline(s, word, ','))
                    data.push_back(word);

                for (int i = 0; i < data.size(); i++) {
                    primaryKeyIndex.push_back(attrib_index_map[data[i]].first);
                    if (i > 0)
                        valueInOther += ",";
                    if (attrib_index_map[data[i]].second == "integer")
                        valueInOther += to_string(*(int*)values[attrib_index_map[data[i]].first]);
                    else if (attrib_index_map[data[i]].second == "string")
                        valueInOther += *(string*)values[attrib_index_map[data[i]].first];
                    else if (attrib_index_map[data[i]].second == "float")
                        valueInOther += to_string(*(float*)values[attrib_index_map[data[i]].first]);
                }
            }
            else
            {
                string word = "";
                stringstream s(line);
                vector<string> data;

                while (getline(s, word, char(170)))
                    data.push_back(word);

                attrib_index_map[data[0]] = { idx, data[1] };

                if (idx > 0)
                    valueInPK += char(170);

                if (data[1] == "integer")
                    valueInPK += to_string(*(int*)values[idx]);
                else if (data[1] == "string")
                    valueInPK += *(string*)values[idx];
                else if (data[1] == "float")
                    valueInPK += to_string(*(float*)values[idx]);
                idx++;
            }
        }

        file.close();
    }
    valueInPK += "}";

    getNewTempFolderIndex();
    filesystem::create_directory("temp/temporary");
    bool primaryKeyIsPresent = false;
    if (primaryKeyIndex.size() == 1)
    {
        for (auto& e : attrib_index_map)
        {
            if (e.second.first == primaryKeyIndex[0])
            {
                if (e.second.second == "string")
                    search_for_field<string>(table_name + "/" + e.first, "=", *(string*)values[e.second.first], "temp/temporary");
                else if (e.second.second == "integer")
                    search_for_field<int>(table_name + "/" + e.first, "=", *(int*)values[e.second.first], "temp/temporary");
                else if (e.second.second == "float")
                    search_for_field<float>(table_name + "/" + e.first, "=", *(float*)values[e.second.first], "temp/temporary");

                break;
            }
        }
    }
    else
    {
        search_for_field<string>(table_name + "/" + primaryKeyName, "=", valueInOther, "temp/temporary");
    }
    ifstream primaryKeyPresent("temp/temporary/pages_0.txt");
    if (primaryKeyPresent.is_open())
    {
        primaryKeyIsPresent = true;
        primaryKeyPresent.close();
    }
    filesystem::remove_all("temp/temporary");

    if (primaryKeyIsPresent)
    {
        std::cout << "Primary key with this value is already present in database!\n";
        return false;
    }

    file.open(table_name + "/meta.txt");
    if (file.is_open()) {
        string line;
        int idx = 0;

        while (getline(file, line)) {
            if (line.substr(0, 12) == "PrimaryKey: ")
                continue;
            string word = "";
            stringstream s(line);
            vector<string> data;

            while (getline(s, word, char(170)))
                data.push_back(word);

            string additionalData = "";
            if (primaryKeyIndex.size() == 1 && primaryKeyIndex[0] == idx)
                additionalData = valueInPK;
            else
                additionalData = valueInOther;

            if (data[1] == "integer")
                insert_value_for_field<int>(table_name + "/" + data[0], *(int*)values[idx], additionalData);
            else if (data[1] == "string")
                insert_value_for_field<string>(table_name + "/" + data[0], *(string*)values[idx], additionalData);
            else if (data[1] == "float")
                insert_value_for_field<float>(table_name + "/" + data[0], *(float*)values[idx], additionalData);

            idx++;
        }
        file.close();

        if (primaryKeyIndex.size() > 1)
            insert_value_for_field<string>(table_name + "/" + primaryKeyName, valueInOther, valueInPK);
    }
    else
    {
        cout << "Could not read table meta.txt\n";
        return false;
    }
    return true;
}

bool insert_entry_in_table(string table_name, string csvFileLocation, bool hasPK = true, bool ignoreFirstLine = true)
{
    unordered_map<int, string> attrib_types;
    ifstream table_metaFile("databases/" + table_name + "/meta.txt");
    if (table_metaFile.is_open())
    {
        string line;
        int col_idx = 0;
        while (getline(table_metaFile, line))
        {
            if (line.substr(0, 12) == "PrimaryKey: ")
                continue;
            string word;
            stringstream ss(line);
            vector<string> data;
            while (getline(ss, word, char(170)))
                data.push_back(word);

            attrib_types[col_idx] = data[1];
            col_idx++;
        }
        table_metaFile.close();
    }

    ifstream readCSV(csvFileLocation);
    if (readCSV.is_open())
    {
        string line;
        int row = 0;

        while (getline(readCSV, line))
        {
            if (row == 0 && ignoreFirstLine)
            {
                row = 1;
                continue;
            }

            vector<void*> inputData;
            int PK = row;
            if (!hasPK)
                inputData.push_back((void*)&PK);

            vector<int> data_int(50);
            vector<float> data_float(50);
            vector<string> data_string(50);

            stringstream ss(line);
            string word;
            int col_idx = 0;
            if (!hasPK)
                col_idx++;
            while (getline(ss, word, ','))
            {
                if(attrib_types[col_idx] == "integer")
                {
                    data_int[col_idx] = stoi(word);
                    inputData.push_back((void*)&data_int[col_idx]);
                }
                else if (attrib_types[col_idx] == "string")
                {
                    data_string[col_idx] = word;
                    inputData.push_back((void*)&data_string[col_idx]);
                }
                else if (attrib_types[col_idx] == "float")
                {
                    data_float[col_idx] = stof(word);
                    inputData.push_back((void*)&data_float[col_idx]);
                }
                col_idx++;
            }

            if (!insert_entry_in_table(table_name, inputData))
                return false;
            row++;
        }

        cout << "Inserted " << row << " Entries" << endl;
    }

    return true;
}

bool search_in_table(string table_name, vector<void*> attribValToSearch, vector<string> projectFields, bool printValues = true) {
    table_name = "databases/" + table_name;
    ifstream file(table_name + "/meta.txt");
    string primaryKeyName;
    vector<int> primaryKeyIndices;
    unordered_map<string, pair<int, string>> attrib_index_map;

    if (file.is_open())
    {
        int idx = 0;
        string line;
        while (getline(file, line)) {
            if (line.substr(0, min(12, (int)line.size())) == "PrimaryKey: ") {
                string primaryKey = line.substr(12, line.size() - 12);
                primaryKeyName = primaryKey;
                stringstream ss(primaryKeyName);
                string word;
                while (getline(ss, word, ',')) {
                    primaryKeyIndices.push_back(attrib_index_map[word].first);
                }
            }
            else {
                string word = "";
                stringstream s(line);
                vector<string> data;

                while (getline(s, word, char(170)))
                    data.push_back(word);

                attrib_index_map[data[0]] = { idx, data[1] };
                idx++;
            }
        }
        file.close();
    }
    int start = getNewTempFolderIndex();

    string outputFolder = "temp/temp_" + to_string(start);
    filesystem::create_directory(outputFolder);
    ofstream outputMetaFile(outputFolder + "/meta.txt");
    for (int i = 0; i < projectFields.size(); i++) {
        outputMetaFile << attrib_index_map[projectFields[i]].first << char(170) << projectFields[i] << char(170) << attrib_index_map[projectFields[i]].second << endl;
    }
    if (!projectFields.size()) {
        vector<pair<int, string>> headers;

        for (auto& e : attrib_index_map)
            headers.push_back({ e.second.first, e.first });

        std::sort(headers.begin(), headers.end());

        for (auto& e : headers)
        {
            outputMetaFile << attrib_index_map[e.second].first << char(170) << e.second << char(170) << attrib_index_map[e.second].second << endl;
        }
    }
    outputMetaFile << "PrimaryKey: ";
    for (int i = 0; i < primaryKeyIndices.size(); i++) {
        outputMetaFile << primaryKeyIndices[i];
        if (i + 1 < primaryKeyIndices.size())
            outputMetaFile << ",";
    }
    outputMetaFile << endl;
    outputMetaFile.close();

    int entryPages = 0;

    if (attribValToSearch.size() == 0) {
        entryPages = search_for_field<string>(table_name + "/" + primaryKeyName, outputFolder);
    }
    else if (*(string*)attribValToSearch[0] == primaryKeyName) {
        string fieldName = *(string*)attribValToSearch[0];
        string operation = *(string*)attribValToSearch[1];
        if (attrib_index_map[fieldName].second == "string")
            entryPages = search_for_field<string>(table_name + "/" + fieldName, operation, *(string*)attribValToSearch[2], outputFolder);
        else if (attrib_index_map[fieldName].second == "integer")
            entryPages = search_for_field<int>(table_name + "/" + fieldName, operation, *(int*)attribValToSearch[2], outputFolder);
        else if (attrib_index_map[fieldName].second == "float")
            entryPages = search_for_field<float>(table_name + "/" + fieldName, operation, *(float*)attribValToSearch[2], outputFolder);
    }
    else
    {
        string outputFolder1 = "temp/temp_" + to_string(start + 1);
        filesystem::create_directory(outputFolder1);

        string fieldName = *(string*)attribValToSearch[0];
        string operation = *(string*)attribValToSearch[1];
        int numPages = 0;

        if (attrib_index_map[fieldName].second == "string")
            numPages = search_for_field<string>(table_name + "/" + fieldName, operation, *(string*)attribValToSearch[2], outputFolder1);
        else if (attrib_index_map[fieldName].second == "integer")
            numPages = search_for_field<int>(table_name + "/" + fieldName, operation, *(int*)attribValToSearch[2], outputFolder1);
        else if (attrib_index_map[fieldName].second == "float")
            numPages = search_for_field<float>(table_name + "/" + fieldName, operation, *(float*)attribValToSearch[2], outputFolder1);
        numPages++;

        int writeTo = 0;
        for (int page = 0; page < numPages; page++) {
            ifstream primaryKeySearch(outputFolder1 + "/pages_" + to_string(page) + ".txt");
            string line;
            if (primaryKeySearch) {
                while (getline(primaryKeySearch, line)) {
                    if (attrib_index_map.find(primaryKeyName) != attrib_index_map.end()) {
                        if (attrib_index_map[primaryKeyName].second == "string")
                            writeTo = search_for_field<string>(table_name + "/" + primaryKeyName, "=", line, outputFolder, writeTo);
                        else if (attrib_index_map[primaryKeyName].second == "integer")
                            writeTo = search_for_field<int>(table_name + "/" + primaryKeyName, "=", stoi(line), outputFolder, writeTo);
                        else if (attrib_index_map[primaryKeyName].second == "float")
                            writeTo = search_for_field<float>(table_name + "/" + primaryKeyName, "=", stof(line), outputFolder, writeTo);
                    }
                    else
                        writeTo = search_for_field<string>(table_name + "/" + primaryKeyName, "=", line, outputFolder, writeTo);
                }
                primaryKeySearch.close();
            }
        }
        entryPages = writeTo;
        filesystem::remove_all(outputFolder1);
    }

    if (printValues)
    {
        printProjectedValue("temp/temp_" + to_string(start));
        filesystem::remove_all(outputFolder);
    }
    else
    {
        ofstream metaFile("temp/meta.txt");
        metaFile << "Folder_Index: " << start+1 << endl;
        metaFile.close();
    }

    return true;
}


bool delete_entry_in_table(string table_name, vector<void*> whereClause) {
    int start = getNewTempFolderIndex();
    search_in_table(table_name, whereClause, {}, false);

    string tempFolder_Name = "temp/temp_" + to_string(start);

    int i = 0;
    ifstream readTempFiles;
    vector<int> primaryKeyIndex;
    unordered_map<int, pair<string, string>> attrib_index_map;
    ifstream readmetaFile(tempFolder_Name+"/meta.txt");
    if (readmetaFile.is_open()) {
        string line;
        while (getline(readmetaFile, line)) {
            if (line.substr(0, min(12, (int)line.size())) == "PrimaryKey: ")
            {
                string primaryKey = line.substr(12, line.size() - 12);
                stringstream ss(primaryKey);
                string word;
                while (getline(ss, word, ','))
                    primaryKeyIndex.push_back(stoi(word));
                continue;
            }
            string word;
            stringstream ss(line);
            vector<string> data;
            while (getline(ss, word, char(170))) {
                data.push_back(word);
            }
            attrib_index_map[stoi(data[0])] = {data[1], data[2]};
        }
        readmetaFile.close();
    }

    while (true) {
        readTempFiles.open(tempFolder_Name + "/pages_" + to_string(i) + ".txt");
        if (readTempFiles.is_open()) {
            string line;
            while (getline(readTempFiles, line)) {
                string inputReceived = line.substr(1, line.size() - 2);
                stringstream ss(inputReceived);
                string word;
                vector<string> data;
                while (getline(ss, word, char(170)))
                    data.push_back(word);
                bool deletedPrimaryKey = false;

                for (int i = 0; i < data.size(); i++) {
                    string additionalInfo = "";
                    if (primaryKeyIndex.size() == 1 && primaryKeyIndex[0]==i)
                    {
                        additionalInfo = line;
                        deletedPrimaryKey = true;
                    }
                    else
                    {
                        for (int pk = 0; pk < primaryKeyIndex.size(); pk++) {
                            if (additionalInfo == "")
                                additionalInfo = data[primaryKeyIndex[pk]];
                            else
                                additionalInfo += "," + data[primaryKeyIndex[pk]];
                        }
                    }

                    if (attrib_index_map[i].second == "string")
                        delete_for_field<string>("databases/" + table_name + "/" + attrib_index_map[i].first, data[i], additionalInfo);
                    else if (attrib_index_map[i].second == "integer")
                        delete_for_field<int>("databases/" + table_name + "/" + attrib_index_map[i].first, stoi(data[i]), additionalInfo);
                    else if (attrib_index_map[i].second == "float")
                        delete_for_field<float>("databases/" + table_name + "/" + attrib_index_map[i].first, stof(data[i]), additionalInfo);

                }

                if (!deletedPrimaryKey) {
                    string primaryKey = "";
                    string value = "";

                    for (int i = 0; i < primaryKeyIndex.size(); i++) {
                        if (primaryKey == "") {
                            primaryKey = attrib_index_map[primaryKeyIndex[i]].first;
                            value = data[primaryKeyIndex[i]];
                        }
                        else {
                            primaryKey += "," + attrib_index_map[primaryKeyIndex[i]].first;
                            value += "," + data[primaryKeyIndex[i]];
                        }
                    }

                    delete_for_field<string>("databases/" + table_name + "/" + primaryKey, data[i], line);
                }
            }

            readTempFiles.close();
        }
        else
            break;
        i++;
    }
    filesystem::remove_all(tempFolder_Name);
    ofstream readMetaFile("temp/meta.txt");
    readMetaFile << "Folder_Index: " << start << endl;
    readMetaFile.close();

    search_in_table(table_name, {}, {}, true);
    return true;
}

bool update_entry_in_table(string table_name, vector<void*> whereClause, vector<void*> setClause) {
    int start = getNewTempFolderIndex();
    search_in_table(table_name, whereClause, {}, false);

    string tempFolder_Name = "temp/temp_" + to_string(start);

    int i = 0;
    ifstream readTempFiles;
    vector<int> primaryKeyIndex;
    unordered_map<int, pair<string, string>> attrib_index_map;
    unordered_map<string, int> attrib_name_map;

    ifstream readmetaFile(tempFolder_Name + "/meta.txt");
    if (readmetaFile.is_open()) {
        string line;
        while (getline(readmetaFile, line)) {
            if (line.substr(0, min(12, (int)line.size())) == "PrimaryKey: ")
            {
                string primaryKey = line.substr(12, line.size() - 12);
                stringstream ss(primaryKey);
                string word;
                while (getline(ss, word, ','))
                    primaryKeyIndex.push_back(stoi(word));
                continue;
            }
            string word;
            stringstream ss(line);
            vector<string> data;
            while (getline(ss, word, char(170))) {
                data.push_back(word);
            }
            attrib_index_map[stoi(data[0])] = { data[1], data[2] };
            attrib_name_map[data[1]] = stoi(data[0]);
        }
        readmetaFile.close();
    }

    while (true) {
        readTempFiles.open(tempFolder_Name + "/pages_" + to_string(i) + ".txt");
        if (readTempFiles.is_open()) {
            string line;
            while (getline(readTempFiles, line)) {
                string inputReceived = line.substr(1, line.size() - 2);
                stringstream ss(inputReceived);
                string word;
                vector<string> data;
                while (getline(ss, word, char(170)))
                    data.push_back(word);
                bool deletedPrimaryKey = false;

                for (int i = 0; i < data.size(); i++) {
                    string additionalInfo = "";
                    if (primaryKeyIndex.size() == 1 && primaryKeyIndex[0] == i)
                    {
                        additionalInfo = line;
                        deletedPrimaryKey = true;
                    }
                    else
                    {
                        for (int pk = 0; pk < primaryKeyIndex.size(); pk++) {
                            if (additionalInfo == "")
                                additionalInfo = data[primaryKeyIndex[pk]];
                            else
                                additionalInfo += "," + data[primaryKeyIndex[pk]];
                        }
                    }

                    if (attrib_index_map[i].second == "string")
                        delete_for_field<string>("databases/" + table_name + "/" + attrib_index_map[i].first, data[i], additionalInfo);
                    else if (attrib_index_map[i].second == "integer")
                        delete_for_field<int>("databases/" + table_name + "/" + attrib_index_map[i].first, stoi(data[i]), additionalInfo);
                    else if (attrib_index_map[i].second == "float")
                        delete_for_field<float>("databases/" + table_name + "/" + attrib_index_map[i].first, stof(data[i]), additionalInfo);
                }

                if (!deletedPrimaryKey) {
                    string primaryKey = "";
                    string value = "";

                    for (int i = 0; i < primaryKeyIndex.size(); i++) {
                        if (primaryKey == "") {
                            primaryKey = attrib_index_map[primaryKeyIndex[i]].first;
                            value = data[primaryKeyIndex[i]];
                        }
                        else {
                            primaryKey += "," + attrib_index_map[primaryKeyIndex[i]].first;
                            value += "," + data[primaryKeyIndex[i]];
                        }
                    }

                    delete_for_field<string>("databases/" + table_name + "/" + primaryKey, data[i], line);
                }

                // Perform insertion with set Values;
                vector<void*> insertData;
                vector<int> integerInsertData(data.size(), 0);
                vector<float> floatInsertData(data.size(), 0);
                vector<string> stringInsertData(data.size(), "");

                for (int i = 0; i < data.size(); i++) {
                    bool foundInSetClause = false;
                    for (int j = 0; j < setClause.size(); j += 2) {
                        string fname = *(string*)setClause[j];
                        if (attrib_name_map.find(fname) != attrib_name_map.end() && attrib_name_map[fname] == i) {
                            if (attrib_index_map[i].second == "integer") {
                                int val = *(int*)setClause[j + 1];
                                integerInsertData[i] = val;
                                insertData.push_back((void*)&integerInsertData[i]);
                            }
                            else if (attrib_index_map[i].second == "string") {
                                string val = *(string*)setClause[j + 1];
                                stringInsertData[i] = val;
                                insertData.push_back((void*)&stringInsertData[i]);
                            }
                            else if (attrib_index_map[i].second == "float") {
                                float val = *(float*)setClause[j + 1];
                                floatInsertData[i] = val;
                                insertData.push_back((void*)&floatInsertData[i]);
                            }
                            foundInSetClause = true;
                            break;
                        }
                    }

                    if (!foundInSetClause) {
                        if (attrib_index_map[i].second == "integer") {
                            int val = stoi(data[i]);
                            integerInsertData[i] = val;
                            insertData.push_back((void*)&integerInsertData[i]);
                        }
                        else if (attrib_index_map[i].second == "string") {
                            stringInsertData[i] = data[i];
                            insertData.push_back((void*)&stringInsertData[i]);
                        }
                        else if (attrib_index_map[i].second == "float") {
                            float val = stof(data[i]);
                            floatInsertData[i] = val;
                            insertData.push_back((void*)&floatInsertData[i]);
                        }
                    }
                }
                insert_entry_in_table(table_name, insertData);
            }

            readTempFiles.close();
        }
        else
            break;
        i++;
    }
    filesystem::remove_all(tempFolder_Name);
    ofstream readMetaFile("temp/meta.txt");
    readMetaFile << "Folder_Index: " << start << endl;
    readMetaFile.close();

    search_in_table(table_name, {}, {}, true);
    return true;
}

bool join_in_table( string table_name1, vector<void*> whereClause1, vector<string> projectFields1,
                    string table_name2, vector<void*> whereClause2, vector<string> projectFields2,
                    vector<string> onClause, bool printValues = true)
{
    int start = getNewTempFolderIndex();

    string outputFolder = "temp/temp_" + to_string(start);
    filesystem::create_directory(outputFolder);
    ofstream metaFile("temp/meta.txt");
    metaFile << "Folder_Index: " << start + 1 << endl;
    metaFile.close();

    bool addedField_table1 = false, addedField_table2 = false;
    if (find(projectFields1.begin(), projectFields1.end(), onClause[0]) == projectFields1.end())
    {
        if (projectFields1.size() != 0)
        {
            projectFields1.push_back(onClause[0]);
            addedField_table1 = true;
        }
    }
    if (find(projectFields2.begin(), projectFields2.end(), onClause[2]) == projectFields2.end())
    {
        if (projectFields2.size() != 0)
        {
            projectFields2.push_back(onClause[2]);
            addedField_table2 = true;
        }
    }

    search_in_table(table_name1, whereClause1, projectFields1, false);
    search_in_table(table_name2, whereClause2, projectFields2, false);

    join_page("temp/temp_" + to_string(start + 1), "temp/temp_" + to_string(start + 2), onClause, "temp/temp_" + to_string(start));

    filesystem::remove_all("temp/temp_" + to_string(start + 1));
    filesystem::remove_all("temp/temp_" + to_string(start + 2));

    unordered_map<string, pair<int, string>> join_attrib_index_map;
    readTempFolderMeta("temp/temp_" + to_string(start) + "/meta.txt", join_attrib_index_map);
    ofstream write_join_meta("temp/temp_" + to_string(start) + "/meta.txt");
    if (write_join_meta.is_open())
    {
        vector<pair<int, string>> headers;

        for (auto& e : join_attrib_index_map)
            headers.push_back({ e.second.first, e.first });

        std::sort(headers.begin(), headers.end());

        for (int i = 0; i < headers.size(); i++)
        {
            string fieldname = headers[i].second.substr(2, headers[i].second.size() - 2);
            if (addedField_table2 && addedField_table1 && fieldname == onClause[2])
                continue;
            if (headers[i].second[0] == 's')
                fieldname = table_name1 + "." + fieldname;
            else
                fieldname = table_name2 + "." + fieldname;
            write_join_meta << headers[i].first << char(170) << fieldname << char(170) << join_attrib_index_map[headers[i].second].second << endl;
        }
        write_join_meta.close();
    }

    if (printValues)
    {
        printProjectedValue("temp/temp_" + to_string(start));
        filesystem::remove_all("temp/temp_" + to_string(start));
        ofstream metaFile("temp/meta.txt");
        metaFile << "Folder_Index: " << start << endl;
        metaFile.close();
    }
    else
    {
        ofstream metaFile("temp/meta.txt");
        metaFile << "Folder_Index: " << start + 1 << endl;
        metaFile.close();
    }

    return true;
}

bool find_in_table(string table_name, vector<string> projectFields, vector<void*> whereClause, pair<string, string> sortingClause, string clusterAttrib, bool printValues = true);
bool join_in_table( string table_name1, vector<string> projectFields1, vector<void*> whereClause1, pair<string, string> sortingClause1, string clusterAttrib1,
                    string table_name2, vector<string> projectFields2, vector<void*> whereClause2, pair<string, string> sortingClause2, string clusterAttrib2,
                    string table_name1_id, string table_name2_id, vector<string> onClause, pair<string, string> finalSort, bool printValues = true)
{
    int start = getNewTempFolderIndex();

    string outputFolder = "temp/temp_" + to_string(start);
    filesystem::create_directory(outputFolder);
    ofstream metaFile("temp/meta.txt");
    metaFile << "Folder_Index: " << start + 1 << endl;
    metaFile.close();

    bool addedField_table1 = false, addedField_table2 = false;
    if (find(projectFields1.begin(), projectFields1.end(), onClause[0]) == projectFields1.end())
    {
        if (projectFields1.size() != 0)
        {
            projectFields1.push_back(onClause[0]);
            addedField_table1 = true;
        }
    }
    if (find(projectFields2.begin(), projectFields2.end(), onClause[2]) == projectFields2.end())
    {
        if (projectFields2.size() != 0)
        {
            projectFields2.push_back(onClause[2]);
            addedField_table2 = true;
        }
    }

    find_in_table(table_name1, projectFields1, whereClause1, sortingClause1, clusterAttrib1, false);
    find_in_table(table_name2, projectFields2, whereClause2, sortingClause2, clusterAttrib2, false);

    join_page("temp/temp_" + to_string(start + 1), "temp/temp_" + to_string(start + 2), onClause, "temp/temp_" + to_string(start));

    filesystem::remove_all("temp/temp_" + to_string(start + 1));
    filesystem::remove_all("temp/temp_" + to_string(start + 2));

    unordered_map<string, pair<int, string>> join_attrib_index_map;
    readTempFolderMeta("temp/temp_" + to_string(start) + "/meta.txt", join_attrib_index_map);
    ofstream write_join_meta("temp/temp_" + to_string(start) + "/meta.txt");
    if (write_join_meta.is_open())
    {
        vector<pair<int, string>> headers;

        for (auto& e : join_attrib_index_map)
            headers.push_back({ e.second.first, e.first });

        std::sort(headers.begin(), headers.end());

        for (int i = 0; i < headers.size(); i++)
        {
            string fieldname = headers[i].second.substr(2, headers[i].second.size() - 2);
            if (addedField_table2 && addedField_table1 && fieldname == onClause[2])
                continue;
            if (headers[i].second[0] == 's')
                fieldname = table_name1_id + "." + fieldname;
            else
                fieldname = table_name2_id + "." + fieldname;
            write_join_meta << headers[i].first << char(170) << fieldname << char(170) << join_attrib_index_map[headers[i].second].second << endl;
        }
        write_join_meta.close();
    }

    if (finalSort.first != "")
        order_page("temp/temp_" + to_string(start), finalSort);

    if (printValues)
    {
        printProjectedValue("temp/temp_" + to_string(start));
        filesystem::remove_all("temp/temp_" + to_string(start));
        ofstream metaFile("temp/meta.txt");
        metaFile << "Folder_Index: " << start << endl;
        metaFile.close();
    }
    else
    {
        ofstream metaFile("temp/meta.txt");
        metaFile << "Folder_Index: " << start + 1 << endl;
        metaFile.close();
    }

    return true;
}

bool order_by_in_table(string table_name, vector<void*> attribValToSearch, vector<string> projectFields, pair<string, string> attribToOrderBy, bool printValues = true) {
    int start = getNewTempFolderIndex();
    search_in_table(table_name, attribValToSearch, projectFields, false);

    string tempFolder_Name = "temp/temp_" + to_string(start);
    order_page(tempFolder_Name, attribToOrderBy);

    if (printValues)
    {
        printProjectedValue("temp/temp_" + to_string(start));
        filesystem::remove_all("temp/temp_" + to_string(start));

        ofstream metaFile("temp/meta.txt");
        metaFile << "Folder_Index: " << start << endl;
        metaFile.close();
    }

    return true;
}

bool group_and_aggregate_in_table(string table_name, vector<void*> whereClause, string groupOnAttribName, vector<pair<string, string>> aggregateOnAttribName, bool printValues = true)
{
    int start = getNewTempFolderIndex();
    vector<string> projectFields;
    projectFields.push_back(groupOnAttribName);
    for (int i = 0; i < aggregateOnAttribName.size(); i++)
    {
        if (find(projectFields.begin(), projectFields.end(), aggregateOnAttribName[i].first) == projectFields.end())
            projectFields.push_back(aggregateOnAttribName[i].first);
    }
    order_by_in_table(table_name, whereClause, projectFields, { groupOnAttribName, "ASC" }, false);

    group_aggregate_page("temp/temp_" + to_string(start), groupOnAttribName, aggregateOnAttribName);

    if (printValues)
    {
        printProjectedValue("temp/temp_" + to_string(start));
        filesystem::remove_all("temp/temp_" + to_string(start));
        ofstream metaFile("temp/meta.txt");
        metaFile << "Folder_Index: " << start << endl;
        metaFile.close();
    }

    return true;
}

bool find_in_table(string table_name, vector<string> projectFields, vector<void*> whereClause, pair<string, string> sortingClause, string clusterAttrib, bool printValues)
{
    //if group by is present call group_and_aggregate_in_table() -> if sorting is present then after grouping, call order_by_in_table()
    //else if sorting is present just call order_by_in_table()
    //else directly call search_in_table()

    if (clusterAttrib != "")
    {
        bool needsSorting = sortingClause.first != "";
        vector<pair<string, string>> aggregateCols;
        for (int i = 0; i < projectFields.size(); i += 2)
        {
            pair<string, string> agg;
            agg.first = projectFields[i];
            agg.second = projectFields[i + 1];
            aggregateCols.push_back(agg);
        }
        int start = getNewTempFolderIndex();
        group_and_aggregate_in_table(table_name, whereClause, clusterAttrib, aggregateCols, false);
        if (needsSorting)
        {
            string tempFolder_Name = "temp/temp_" + to_string(start);
            order_page(tempFolder_Name, sortingClause);
        }
        if (printValues)
        {
            printProjectedValue("temp/temp_" + to_string(start));
            filesystem::remove_all("temp/temp_" + to_string(start));

            ofstream metaFile("temp/meta.txt");
            metaFile << "Folder_Index: " << start << endl;
            metaFile.close();
        }
    }
    else if (sortingClause.first != "")
    {
        order_by_in_table(table_name, whereClause, projectFields, sortingClause, printValues);
    }
    else
    {
        search_in_table(table_name, whereClause, projectFields, printValues);
    }

    return true;
}
