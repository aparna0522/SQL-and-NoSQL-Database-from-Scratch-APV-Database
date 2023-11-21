/*
BTree Implementation, provided functionalities for range search, insertion, deletion
All nodes are saved as files and retrieved in response to avoid loading entire database in main memory
*/

#pragma once

#include <stdio.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "pages.h"

using namespace std;
int numberOfNodes;
string rootName;
string fieldname;

string readFile(const string file_name) {
  ifstream file(fieldname + file_name);
  string file_contents;
  if (file.is_open()) {
    string line;
    while (getline(file, line)) {
      file_contents += line + '\n';
    }
    file.close();
  } else {
    ofstream dataFile(fieldname + file_name);
    if (dataFile.is_open()) {
      for (int i = 0; i < 2 * ORDER - 1; i++) {
        dataFile << std::endl;
        file_contents += '\n';
      }
      dataFile.close();
    } else {
      cerr << "Failed creating a new node for the data file for field: "
           << file_name << endl;
    }
  }
  return file_contents;
}

template <typename T>
T convertValue(string value);

template <>
int convertValue<int>(string value) {
  return stoi(value);
}

template <>
string convertValue<string>(string value) {
  return value;
}

template <>
float convertValue<float>(string value) {
  return stof(value);
}

void writeToPage(vector<string>& data, int& currIndex, string& outputLoc) {
  ifstream checkNoOfLines;
  ofstream outputFile;
  checkNoOfLines.open(outputLoc + "/pages_" + to_string(currIndex) + ".txt");
  int lineCount = 0;

  if (checkNoOfLines.is_open()) {
    string line;
    while (getline(checkNoOfLines, line)) {
      lineCount++;
    }
    checkNoOfLines.close();
    if (lineCount >= CHUNK_SIZE) {
      currIndex++;
      outputFile.open(outputLoc + "/pages_" + to_string(currIndex) + ".txt");
      lineCount = 0;
    } else {
      outputFile.open(outputLoc + "/pages_" + to_string(currIndex) + ".txt",
                      ios_base::app);
    }
  } else {
    outputFile.open(outputLoc + "/pages_" + to_string(currIndex) + ".txt");
    if (data.size() >= CHUNK_SIZE) currIndex++;
  }

  int dataSize = data.size();
  for (int i = 0; i < min(CHUNK_SIZE - lineCount, dataSize); i++) {
    outputFile << data[0] << endl;
    data.erase(data.begin());
  }

  outputFile.close();
}

template <typename T>
struct BTreeNode {
  vector<pair<T, string> > data;
  vector<string> children;
  string type;
  string filename;

  BTreeNode(string filename, string type) {
    this->type = type;
    this->filename = filename;

    string fileContent = readFile("/data_" + filename + ".txt");

    string lineContent = "";
    stringstream s(fileContent);
    int count = 1;
    int childCount = 0;

    while (getline(s, lineContent)) {
      if (count < ORDER) {
        if (lineContent != "") {
          vector<string> content;
          int commaFinder = lineContent.find(char(170));
          content.push_back(lineContent.substr(0, commaFinder));
          content.push_back(lineContent.substr(
              commaFinder + 1, lineContent.size() - commaFinder - 1));

          if (type == "integer")
            data.push_back({convertValue<T>(content[0]), content[1]});
          else if (type == "string")
            data.push_back({convertValue<T>(content[0]), content[1]});
          else if (type == "float")
            data.push_back({convertValue<T>(content[0]), content[1]});
        }
      } else {
        if (childCount < ORDER) children.push_back(lineContent);
        childCount++;
      }
      count++;
    }
  }

  void insert_non_full(T value, string& additionalData) {
    int i = data.size() - 1;
    if (children[0] == "") {
      while (i >= 0 && data[i].first > value) {
        if (i + 1 >= data.size())
          data.push_back(data[i]);
        else
          data[i + 1] = data[i];
        i--;
      }
      if (i + 1 >= data.size())
        data.push_back({value, additionalData});
      else
        data[i + 1] = {value, additionalData};
    } else {
      while (i >= 0 && data[i].first > value) i--;
      BTreeNode child(children[i + 1], type);
      if (child.data.size() >= ORDER - 1) {
        splitChild(i + 1, &child);
        if (data[i + 1].first < value) i++;
        child.updateFile();
      }
      BTreeNode insertNode(children[i + 1], type);
      insertNode.insert_non_full(value, additionalData);
    }
    updateFile();
  }

  void updateFile() {
    if (data.size() == 0 && filename == rootName && children[0] != "") {
      ofstream metaFile(fieldname + "/meta.txt");
      if (metaFile.is_open()) {
        metaFile << "DataType: " << type << std::endl;
        metaFile << "Root: " << children[0] << std::endl;
        metaFile << "Nodes: " << numberOfNodes;
        metaFile.close();
      } else {
        cerr << "Failed to change the root name after root itself got deleted."
             << endl;
      }
      string deleteFileName = fieldname + "/data_" + filename + ".txt";
      remove(deleteFileName.c_str());
      return;
    }

    ofstream dataFile(fieldname + "/data_" + filename + ".txt");
    if (dataFile.is_open()) {
      for (int i = 0; i < ORDER - 1; i++) {
        if (data.size() > i)
          dataFile << data[i].first << char(170) << data[i].second << std::endl;
        else
          dataFile << std::endl;
      }
      for (int i = 0; i < ORDER; i++) {
        dataFile << children[i] << std::endl;
      }
      dataFile.close();
    } else {
      cerr << "Failed updating the data file" << endl;
    }
  }

  void splitChild(int insertPosition, BTreeNode* leftNode) {
    numberOfNodes++;
    string splitRightNodeName = to_string(numberOfNodes);
    BTreeNode rightNode(splitRightNodeName, type);

    int mid = ((ORDER - 1) / 2);

    T midValue = leftNode->data[mid].first;
    string midAdditionalData = leftNode->data[mid].second;

    // Copy right half of y to z
    for (int i = mid + 1; i < leftNode->data.size(); i++) {
      rightNode.data.push_back(leftNode->data[i]);
    }
    // Erase the right half from y
    for (int i = leftNode->data.size() - 1; i >= mid; i--) {
      leftNode->data.erase(leftNode->data.begin() + i);
    }

    if (leftNode->children[0] != "") {
      int rightChildStart = (ORDER / 2);
      // Copy right half children from y to z, set y children to empty
      for (int i = 0; i < (ORDER / 2); i++, rightChildStart++) {
        rightNode.children[i] = leftNode->children[rightChildStart];
        leftNode->children[rightChildStart] = "";
      }
    }

    for (int i = data.size(); i >= insertPosition + 1; i--) {
      children[i + 1] = children[i];
    }
    children[insertPosition + 1] = splitRightNodeName;

    for (int i = data.size() - 1; i >= insertPosition; i--) {
      if (i + 1 >= data.size())
        data.push_back(data[i]);
      else
        data[i + 1] = data[i];
    }
    if (insertPosition < data.size())
      data[insertPosition] = {midValue, midAdditionalData};
    else
      data.push_back(
          {midValue, midAdditionalData});  // Copy middle key of Y to this node

    rightNode.updateFile();
  }

  string insert(T value, string& additionalData) {
    if (data.size() < ORDER - 1) {
      insert_non_full(value, additionalData);
      return filename;
    } else {
      numberOfNodes++;
      string rootName = to_string(numberOfNodes);
      BTreeNode newRoot(rootName, type);
      newRoot.children[0] = filename;
      newRoot.splitChild(0, this);
      updateFile();
      newRoot.updateFile();

      int i = 0;
      if (newRoot.data[i].first < value) i++;
      BTreeNode child(newRoot.children[i], type);
      child.insert_non_full(value, additionalData);

      return rootName;
    }
  }

  void search_less(T value, vector<string>& searchQueryResults,
                   string& output_folder_path, int& current_output_index) {
    int i = 0;
    while (i < data.size() && data[i].first < value) {
      if (children[i] != "") {
        BTreeNode child(children[i], type);
        child.search_less(value, searchQueryResults, output_folder_path,
                          current_output_index);
      }
      searchQueryResults.push_back(data[i].second);
      if (searchQueryResults.size() >= CHUNK_SIZE)
        writeToPage(searchQueryResults, current_output_index,
                    output_folder_path);
      i++;
    }
    /*if (i == data.size()) {*/
    if (children[i] != "") {
      BTreeNode child(children[i], type);
      child.search_less(value, searchQueryResults, output_folder_path,
                        current_output_index);
    }
    //}
    else if (i == 0) {
      if (children[0] != "") {
        BTreeNode child(children[0], type);
        child.search_less(value, searchQueryResults, output_folder_path,
                          current_output_index);
      }
    }
  }

  void search_greater(T value, vector<string>& searchQueryResults,
                      string& output_folder_path, int& current_output_index) {
    int i = 0;
    while (i < data.size() && data[i].first <= value) i++;

    while (i < data.size()) {
      if (children[i] != "") {
        BTreeNode child(children[i], type);
        child.search_greater(value, searchQueryResults, output_folder_path,
                             current_output_index);
      }
      searchQueryResults.push_back(data[i].second);
      if (searchQueryResults.size() >= CHUNK_SIZE)
        writeToPage(searchQueryResults, current_output_index,
                    output_folder_path);

      i++;
    }
    if (i == data.size()) {
      if (children[i] != "") {
        BTreeNode child(children[i], type);
        child.search_greater(value, searchQueryResults, output_folder_path,
                             current_output_index);
      }
    }
  }

  void search_equals(T value, vector<string>& searchQueryResults,
                     string& output_folder_path, int& current_output_index) {
    int i = 0;
    while (i < data.size() && data[i].first < value) i++;

    while (i < data.size() && data[i].first == value) {
      if (children[i] != "") {
        BTreeNode child(children[i], type);
        child.search_equals(value, searchQueryResults, output_folder_path,
                            current_output_index);
      }
      searchQueryResults.push_back(data[i].second);
      if (searchQueryResults.size() >= CHUNK_SIZE)
        writeToPage(searchQueryResults, current_output_index,
                    output_folder_path);

      i++;
    }
    if (children[i] != "") {
      BTreeNode child(children[i], type);
      child.search_equals(value, searchQueryResults, output_folder_path,
                          current_output_index);
    }
  }

  void search(T value, string operation, vector<string>& searchQueryResults,
              string& output_folder_path, int& current_output_index) {
    if (operation == "=")
      search_equals(value, searchQueryResults, output_folder_path,
                    current_output_index);
    else if (operation == "<")
      search_less(value, searchQueryResults, output_folder_path,
                  current_output_index);
    else if (operation == ">")
      search_greater(value, searchQueryResults, output_folder_path,
                     current_output_index);
    else if (operation == "<=") {
      search_less(value, searchQueryResults, output_folder_path,
                  current_output_index);
      search_equals(value, searchQueryResults, output_folder_path,
                    current_output_index);
    } else if (operation == ">=") {
      search_equals(value, searchQueryResults, output_folder_path,
                    current_output_index);
      search_greater(value, searchQueryResults, output_folder_path,
                     current_output_index);
    }
  }

  void traverse(vector<string>& searchQueryResults, string& output_folder_path,
                int& current_output_index) {
    int i;
    for (i = 0; i < data.size(); i++) {
      if (children[i] != "") {
        BTreeNode<T> child(children[i], type);
        child.traverse(searchQueryResults, output_folder_path,
                       current_output_index);
      }
      searchQueryResults.push_back(data[i].second);
      if (searchQueryResults.size() >= CHUNK_SIZE) {
        writeToPage(searchQueryResults, current_output_index,
                    output_folder_path);
      }
    }

    if (children[i] != "") {
      BTreeNode<T> child(children[i], type);
      child.traverse(searchQueryResults, output_folder_path,
                     current_output_index);
    }
  }

  void deleteFromLeaf(int index) {
    for (int i = index + 1; i < data.size(); i++) {
      data[i - 1] = data[i];
    }
    data.erase(data.end() - 1);
    updateFile();
    return;
  }

  pair<T, string> getPred(int index) {
    BTreeNode<T>* child = new BTreeNode<T>(children[index], type);
    while (child->children[0] != "") {
      string idx = child->children[child->data.size()];
      delete (child);
      child = new BTreeNode<T>(idx, type);
    }
    pair<T, string> answer = child->data[child->data.size() - 1];
    delete (child);
    return answer;
  }

  pair<T, string> getSucc(int index) {
    BTreeNode<T>* child = new BTreeNode<T>(children[index + 1], type);
    while (child->children[0] != "") {
      string idx = child->children[0];
      delete (child);
      child = new BTreeNode<T>(idx, type);
    }
    pair<T, string> answer = child->data[0];
    delete (child);
    return answer;
  }

  void merge(int index) {
    int t = (ORDER / 2);
    BTreeNode child(children[index], type);
    BTreeNode sibling(children[index + 1], type);
    child.data.push_back(data[index]);
    for (int i = 0; i < sibling.data.size(); i++) {
      child.data.push_back(sibling.data[i]);
    }
    if (child.children[0] != "") {
      for (int i = 0; i <= sibling.data.size(); i++) {
        child.children[i + t] = (sibling.children[i]);
      }
    }
    for (int i = index + 1; i < data.size(); i++) {
      data[i - 1] = data[i];
    }
    data.erase(data.end() - 1);
    for (int i = index + 2; i <= data.size() + 1; i++) {
      children[i - 1] = children[i];
    }
    children[data.size() + 1] = "";
    child.updateFile();
    string siblingFileName = fieldname + "/data_" + sibling.filename + ".txt";
    remove(siblingFileName.c_str());
    updateFile();
  }

  bool deleteFromNonLeaf(int index) {
    pair<T, string> k = data[index];
    if (children[index] != "") {
      BTreeNode<T> child(children[index], type);
      if (child.data.size() >= ORDER / 2) {
        pair<T, string> pred = getPred(index);
        data[index] = pred;
        updateFile();
        return child.deletion(pred.first, pred.second);
      }
    }
    if (children[index + 1] != "") {
      BTreeNode<T> child(children[index + 1], type);
      if (child.data.size() >= ORDER / 2) {
        pair<T, string> succ = getSucc(index);
        data[index] = succ;
        updateFile();
        return child.deletion(succ.first, succ.second);
      }
    }

    {
      merge(index);
      BTreeNode<T> child(children[index], type);
      return child.deletion(k.first, k.second);
    }
  }

  void borrowFromPrev(int index) {
    BTreeNode child(children[index], type);
    BTreeNode sibling(children[index - 1], type);

    int child_n = child.data.size();
    for (int i = child.data.size() - 1; i >= 0; i--) {
      child.data.push_back(child.data[i]);
    }

    if (child.children[0] != "") {
      for (int i = child_n; i >= 0; i--) {
        child.children[i + 1] = (child.children[i]);
      }
    }
    child.data[0] = data[index - 1];
    if (child.children[0] != "") {
      child.children[0] = sibling.children[sibling.data.size()];
      sibling.children[sibling.data.size()] = "";
    }

    data[index - 1] = sibling.data[sibling.data.size() - 1];
    sibling.data.erase(sibling.data.end() - 1);

    child.updateFile();
    sibling.updateFile();
  }

  void borrowFromNext(int index) {
    BTreeNode child(children[index], type);
    BTreeNode sibling(children[index + 1], type);

    child.data.push_back(data[index]);

    if (child.children[0] != "") {
      child.children[child.data.size()] = (sibling.children[0]);
    }

    data[index] = sibling.data[0];

    for (int i = 1; i < sibling.data.size(); i++) {
      sibling.data[i - 1] = sibling.data[i];
    }
    sibling.data.erase(sibling.data.end() - 1);

    if (sibling.children[0] != "") {
      for (int i = 1; i <= sibling.data.size() + 1; i++) {
        sibling.children[i - 1] = sibling.children[i];
      }
      sibling.children[sibling.data.size() + 1] = "";
    }
    child.updateFile();
    sibling.updateFile();
  }

  void fill(int index) {
    if (index != 0) {
      BTreeNode<T> child(children[index - 1], type);
      if (child.data.size() >= ORDER / 2) {
        borrowFromPrev(index);
        return;
      }
    }
    if (index != data.size()) {
      BTreeNode<T> child(children[index + 1], type);
      if (child.data.size() >= ORDER / 2) {
        borrowFromNext(index);
        return;
      }
    }

    if (index != data.size())
      merge(index);
    else
      merge(index - 1);
    return;
  }

  bool deletion(T originalVal, string origAdditionalInfo) {
    int i = 0;
    while (i < data.size() && data[i].first < originalVal) i++;
    int present = i;
    while (present < data.size()) {
      if (data[present].first == originalVal &&
          data[present].second == origAdditionalInfo) {
        if (children[0] == "")
          deleteFromLeaf(present);
        else
          deleteFromNonLeaf(present);
        return true;
      }
      present++;
    }
    if (children[0] == "") return false;
    bool flag = i == data.size();
    if (children[i] != "") {
      BTreeNode<T> child(children[i], type);
      if (child.data.size() < ORDER / 2) fill(i);
    }
    updateFile();

    if (flag && i > data.size()) {
      BTreeNode<T> child(children[i - 1], type);
      return child.deletion(originalVal, origAdditionalInfo);
    } else {
      BTreeNode<T> child(children[i], type);
      return child.deletion(originalVal, origAdditionalInfo);
    }
  }
};

template <typename T>
bool insert_value_for_field(string field_name, T value, string additionalData) {
  fieldname = field_name;
  string fileContent = readFile("/meta.txt");

  string root_file = "";
  string attrib_type = "";

  string lineContent = "";
  stringstream s(fileContent);

  while (getline(s, lineContent)) {
    stringstream lc(lineContent);
    string word;

    vector<string> data;
    while (lc >> word) data.push_back(word);

    if (data[0] == "Root:")
      root_file = data[1];
    else if (data[0] == "DataType:")
      attrib_type = data[1];
    else if (data[0] == "Nodes:")
      numberOfNodes = stoi(data[1]);
  }

  BTreeNode<T> root(root_file, attrib_type);
  root_file = root.insert(value, additionalData);
  ofstream metaFile(field_name + "/meta.txt");
  if (metaFile.is_open()) {
    metaFile << "DataType: " << attrib_type << std::endl;
    metaFile << "Root: " << root_file << std::endl;
    metaFile << "Nodes: " << numberOfNodes;
    metaFile.close();
  } else {
    cerr << "Failed writing the meta data for the attribute meta.txt" << endl;
  }

  return true;
}

template <typename T>
int search_for_field(string field_name, string operation, T value,
                     string output_folder_path, int current_output_index = 0) {
  fieldname = field_name;
  string fileContent = readFile("/meta.txt");

  string root_file = "";
  string attrib_type = "";

  string lineContent = "";
  stringstream s(fileContent);

  while (getline(s, lineContent)) {
    stringstream lc(lineContent);
    string word;

    vector<string> data;
    while (lc >> word) data.push_back(word);

    if (data[0] == "Root:")
      root_file = data[1];
    else if (data[0] == "DataType:")
      attrib_type = data[1];
    else if (data[0] == "Nodes:")
      numberOfNodes = stoi(data[1]);
  }

  BTreeNode<T> root(root_file, attrib_type);
  vector<string> searchedQueryResult;
  root.search(value, operation, searchedQueryResult, output_folder_path,
              current_output_index);

  if (searchedQueryResult.size() > 0) {
    writeToPage(searchedQueryResult, current_output_index, output_folder_path);
  }

  return current_output_index;
}

template <typename T>
int search_for_field(string field_name, string output_folder_path,
                     int current_output_index = 0) {
  fieldname = field_name;
  string fileContent = readFile("/meta.txt");

  string root_file = "";
  string attrib_type = "";

  string lineContent = "";
  stringstream s(fileContent);

  while (getline(s, lineContent)) {
    stringstream lc(lineContent);
    string word;

    vector<string> data;
    while (lc >> word) data.push_back(word);

    if (data[0] == "Root:")
      root_file = data[1];
    else if (data[0] == "DataType:")
      attrib_type = data[1];
    else if (data[0] == "Nodes:")
      numberOfNodes = stoi(data[1]);
  }

  BTreeNode<T> root(root_file, attrib_type);
  vector<string> searchedQueryResult;
  root.traverse(searchedQueryResult, output_folder_path, current_output_index);
  if (searchedQueryResult.size() > 0) {
    writeToPage(searchedQueryResult, current_output_index, output_folder_path);
  }

  return current_output_index;
}

template <typename T>
void update_for_field(string field_name, T originalVal,
                      string origAdditionalInfo, T targetVal,
                      string targetAdditionalInfo) {
  fieldname = field_name;
  string fileContent = readFile("/meta.txt");

  string root_file = "";
  string attrib_type = "";

  string lineContent = "";
  stringstream s(fileContent);

  while (getline(s, lineContent)) {
    stringstream lc(lineContent);
    string word;

    vector<string> data;
    while (lc >> word) data.push_back(word);

    if (data[0] == "Root:")
      root_file = data[1];
    else if (data[0] == "DataType:")
      attrib_type = data[1];
    else if (data[0] == "Nodes:")
      numberOfNodes = stoi(data[1]);
  }
  BTreeNode<T> root(root_file, attrib_type);
  vector<string> searchedQueryResult;
  root.update(originalVal, origAdditionalInfo, targetVal, targetAdditionalInfo);
}

template <typename T>
void delete_for_field(string field_name, T originalVal,
                      string origAdditionalInfo) {
  fieldname = field_name;
  string fileContent = readFile("/meta.txt");

  string root_file = "";
  string attrib_type = "";

  string lineContent = "";
  stringstream s(fileContent);

  while (getline(s, lineContent)) {
    stringstream lc(lineContent);
    string word;

    vector<string> data;
    while (lc >> word) data.push_back(word);

    if (data[0] == "Root:")
      root_file = data[1];
    else if (data[0] == "DataType:")
      attrib_type = data[1];
    else if (data[0] == "Nodes:")
      numberOfNodes = stoi(data[1]);
  }
  BTreeNode<T> root(root_file, attrib_type);
  rootName = root_file;
  root.deletion(originalVal, origAdditionalInfo);
}