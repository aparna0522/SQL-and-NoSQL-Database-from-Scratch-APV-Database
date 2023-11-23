/*
Provided processing functions on temp folder to do
- ordering (external merge sort)
- joining (NFL algorithm)
- group by and aggregate
*/

#pragma once
#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common.h"

using namespace std;

int page_chunk_size = CHUNK_SIZE;

void loadPageInMemory(string page_file, vector<vector<string> >& outData,
                      bool deleteThisPage = true) {
  ifstream page(page_file);
  if (page.is_open()) {
    string line;
    while (getline(page, line)) {
      vector<string> data;
      line = line.substr(1, line.size() - 2);
      stringstream ss(line);
      string word;
      while (getline(ss, word, char(170))) data.push_back(word);
      outData.push_back(data);
    }
    page.close();
    if (deleteThisPage) remove(page_file.c_str());
  }
}

void readTempFolderMeta(
    string metaFile,
    unordered_map<string, pair<int, string> >& attrib_index_map) {
  ifstream readmetaFile(metaFile);
  if (readmetaFile.is_open()) {
    string line;
    while (getline(readmetaFile, line)) {
      if (line.substr(0, min(12, (int)line.size())) == "PrimaryKey: ") continue;
      string word;
      stringstream ss(line);
      vector<string> data;
      while (getline(ss, word, char(170))) {
        data.push_back(word);
      }
      attrib_index_map[data[1]] = {stoi(data[0]), data[2]};
    }
    readmetaFile.close();
  }
}

// Write a function - search (sortedNextPass.close())

void combineEntries(vector<string>& entryInTable1,
                    vector<string>& entryInTable2, int commonAttrib,
                    vector<vector<string> >& output, int& outputPageIndex,
                    string& outputFolder) {
  vector<string> entry;
  for (int i = 0; i < entryInTable1.size(); i++)
    if (i != commonAttrib) entry.push_back(entryInTable1[i]);
  for (int i = 0; i < entryInTable2.size(); i++)
    entry.push_back(entryInTable2[i]);

  output.push_back(entry);
  if (output.size() >= page_chunk_size) {
    ofstream pageWrite(outputFolder + "/pages_" + to_string(outputPageIndex++) +
                       ".txt");
    if (pageWrite.is_open()) {
      for (auto& e : output) {
        pageWrite << "{";
        for (int k = 0; k < e.size(); k++) {
          pageWrite << e[k];
          if (k + 1 < e.size())
            pageWrite << char(170);
          else
            pageWrite << "}" << endl;
        }
      }
      pageWrite.close();
    }
    output.clear();
  }
}

bool join_page(string table1FolderName, string table2FolderName,
               vector<string> onClause, string outputFolderName) {
  unordered_map<string, pair<int, string> > attrib_index_map1;
  unordered_map<string, pair<int, string> > attrib_index_map2;

  readTempFolderMeta(table1FolderName + "/meta.txt", attrib_index_map1);
  readTempFolderMeta(table2FolderName + "/meta.txt", attrib_index_map2);

  ifstream table1Page;
  ifstream table2Page;

  // Page's file pointer
  int table1PageIndex = 0, table2PageIndex = 0;

  // This is the column Number Of the on Clause Attrib on which we are joining.
  int onClauseAttrib1 = attrib_index_map1[onClause[0]].first;
  int onClauseAttrib2 = attrib_index_map2[onClause[2]].first;

  vector<vector<string> > output;
  vector<vector<string> > table1PageData;
  vector<vector<string> > table2PageData;

  int table1EntrySize = 0;

  int output_page_index = 0;

  while (true) {
    table1Page.open(table1FolderName + "/pages_" + to_string(table1PageIndex) +
                    ".txt");
    if (table1Page.is_open()) {
      table1PageData.clear();
      loadPageInMemory(
          table1FolderName + "/pages_" + to_string(table1PageIndex) + ".txt",
          table1PageData, false);

      table2PageIndex = 0;
      while (true) {
        table2Page.open(table2FolderName + "/pages_" +
                        to_string(table2PageIndex) + ".txt");
        if (table2Page.is_open()) {
          table2PageData.clear();
          loadPageInMemory(table2FolderName + "/pages_" +
                               to_string(table2PageIndex) + ".txt",
                           table2PageData, false);
          for (int i = 0; i < table1PageData.size(); i++) {
            table1EntrySize = table1PageData[i].size();
            for (int j = 0; j < table2PageData.size(); j++) {
              if (attrib_index_map1[onClause[0]].second == "integer") {
                if (onClause[1] == "=") {
                  if (stoi(table1PageData[i][onClauseAttrib1]) ==
                      stoi(table2PageData[j][onClauseAttrib2]))
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == "<") {
                  if (stoi(table1PageData[i][onClauseAttrib1]) <
                      stoi(table2PageData[j][onClauseAttrib2]))
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == ">") {
                  if (stoi(table1PageData[i][onClauseAttrib1]) >
                      stoi(table2PageData[j][onClauseAttrib2]))
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == "<=") {
                  if (stoi(table1PageData[i][onClauseAttrib1]) <=
                      stoi(table2PageData[j][onClauseAttrib2]))
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == ">=") {
                  if (stoi(table1PageData[i][onClauseAttrib1]) >=
                      stoi(table2PageData[j][onClauseAttrib2]))
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                }
              } else if (attrib_index_map1[onClause[0]].second == "string") {
                if (onClause[1] == "=") {
                  if (table1PageData[i][onClauseAttrib1] ==
                      table2PageData[j][onClauseAttrib2])
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == "<") {
                  if (table1PageData[i][onClauseAttrib1] <
                      table2PageData[j][onClauseAttrib2])
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == ">") {
                  if (table1PageData[i][onClauseAttrib1] >
                      table2PageData[j][onClauseAttrib2])
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == "<=") {
                  if (table1PageData[i][onClauseAttrib1] <=
                      table2PageData[j][onClauseAttrib2])
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == ">=") {
                  if (table1PageData[i][onClauseAttrib1] >=
                      table2PageData[j][onClauseAttrib2])
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                }

              } else if (attrib_index_map1[onClause[0]].second == "float") {
                if (onClause[1] == "=") {
                  if (stof(table1PageData[i][onClauseAttrib1]) ==
                      stof(table2PageData[j][onClauseAttrib2]))
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == "<") {
                  if (stof(table1PageData[i][onClauseAttrib1]) <
                      stof(table2PageData[j][onClauseAttrib2]))
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == ">") {
                  if (stof(table1PageData[i][onClauseAttrib1]) >
                      stof(table2PageData[j][onClauseAttrib2]))
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == "<=") {
                  if (stof(table1PageData[i][onClauseAttrib1]) <=
                      stof(table2PageData[j][onClauseAttrib2]))
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                } else if (onClause[1] == ">=") {
                  if (stof(table1PageData[i][onClauseAttrib1]) >=
                      stof(table2PageData[j][onClauseAttrib2]))
                    combineEntries(table1PageData[i], table2PageData[j],
                                   onClauseAttrib1, output, output_page_index,
                                   outputFolderName);
                }
              }
            }
          }

          table2Page.close();
        } else
          break;
        table2PageIndex++;
      }

      table1Page.close();
    } else
      break;

    table1PageIndex++;
  }

  if (output.size() > 0) {
    ofstream pageWrite(outputFolderName + "/pages_" +
                       to_string(output_page_index++) + ".txt");
    if (pageWrite.is_open()) {
      for (auto& e : output) {
        pageWrite << "{";
        for (int k = 0; k < e.size(); k++) {
          pageWrite << e[k];
          if (k + 1 < e.size())
            pageWrite << char(170);
          else
            pageWrite << "}" << endl;
        }
      }
      pageWrite.close();
    }
    output.clear();
  }

  ofstream joinMeta(outputFolderName + "/meta.txt");
  if (joinMeta.is_open()) {
    vector<pair<int, string> > headers;

    for (auto& e : attrib_index_map1)
      headers.push_back({e.second.first, e.first});

    std::sort(headers.begin(), headers.end());

    int decrementer = 0;
    for (int i = 0; i < headers.size(); i++) {
      if (headers[i].first == onClauseAttrib1) {
        decrementer -= 1;
        continue;
      }
      joinMeta << headers[i].first + decrementer << char(170) << "s."
               << headers[i].second << char(170)
               << attrib_index_map1[headers[i].second].second << endl;
    }

    headers.clear();

    for (auto& e : attrib_index_map2)
      headers.push_back({e.second.first, e.first});

    std::sort(headers.begin(), headers.end());
    table1EntrySize--;

    for (int i = 0; i < headers.size(); i++) {
      joinMeta << headers[i].first + table1EntrySize << char(170) << "t."
               << headers[i].second << char(170)
               << attrib_index_map2[headers[i].second].second << endl;
    }

    headers.clear();

    joinMeta.close();
  }

  return true;
}

// attribToOrderBy : sid ASC
// temp
//   - meta.txt (Folder Name: <INT>)
//   - temp_0
//        - meta.txt (All the attributes to be projected and their indexes wrt
//        the table) (0 sid integer \n 1 name string \n 2 address string)
//        - pages_0.txt (Apply bubble sort to sort a particular page) \
//                                           (Merge both of them together using
//                                           merge sort, but keep the chunks as
//                                           it is)
//        - pages_1.txt (Apply bubble sort to sort a particular page) /
bool order_page(string tempFolder_Name, pair<string, string> attribToOrderBy) {
  int sortIndex = -1;
  string typeOfSortIndex = "";
  ifstream readmetaFile(tempFolder_Name + "/meta.txt");
  if (readmetaFile.is_open()) {
    string line;
    while (getline(readmetaFile, line)) {
      if (line.substr(0, min(12, (int)line.size())) == "PrimaryKey: ") continue;
      string word;
      stringstream ss(line);
      vector<string> data;
      while (getline(ss, word, char(170))) {
        data.push_back(word);
      }
      if (data[1] == attribToOrderBy.first) {
        sortIndex = stoi(data[0]);
        typeOfSortIndex = data[2];
        break;
      }
    }
    readmetaFile.close();
  }

  if (sortIndex == -1) return false;

  int i = 0;
  int elements = 0;

  while (true) {
    ifstream readPagesInFolder(tempFolder_Name + "/pages_" + to_string(i) +
                               ".txt");
    if (readPagesInFolder.is_open()) {
      vector<vector<string> > page;
      string line;
      while (getline(readPagesInFolder, line)) {
        elements++;
        line = line.substr(1, line.size() - 2);
        stringstream ss(line);
        string word;
        vector<string> entry;
        while (getline(ss, word, char(170))) entry.push_back(word);
        page.push_back(entry);
      }
      readPagesInFolder.close();

      bool swapped;
      for (int j = 0; j < page.size(); j++) {
        swapped = false;
        for (int k = 0; k < page.size() - j - 1; k++) {
          if (typeOfSortIndex == "string") {
            if ((attribToOrderBy.second == "ASC" &&
                 page[k][sortIndex] > page[k + 1][sortIndex]) ||
                (attribToOrderBy.second == "DESC" &&
                 page[k][sortIndex] < page[k + 1][sortIndex])) {
              swap(page[k], page[k + 1]);
              swapped = true;
            }
          } else if (typeOfSortIndex == "integer") {
            if ((attribToOrderBy.second == "ASC" &&
                 stoi(page[k][sortIndex]) > stoi(page[k + 1][sortIndex])) ||
                (attribToOrderBy.second == "DESC" &&
                 stoi(page[k][sortIndex]) < stoi(page[k + 1][sortIndex]))) {
              swap(page[k], page[k + 1]);
              swapped = true;
            }
          } else if (typeOfSortIndex == "float") {
            if ((attribToOrderBy.second == "ASC" &&
                 stof(page[k][sortIndex]) > stof(page[k + 1][sortIndex])) ||
                (attribToOrderBy.second == "DESC" &&
                 stof(page[k][sortIndex]) < stof(page[k + 1][sortIndex]))) {
              swap(page[k], page[k + 1]);
              swapped = true;
            }
          }
        }
        if (!swapped) break;
      }

      ofstream writePage(tempFolder_Name + "/pages_" + to_string(i) + "_0.txt");
      for (int pg = 0; pg < page.size(); pg++) {
        writePage << "{";
        for (int k = 0; k < page[pg].size(); k++) {
          writePage << page[pg][k];
          if (k + 1 < page[pg].size()) writePage << char(170);
        }
        writePage << "}" << endl;
      }
      writePage.close();
      string originalFN = tempFolder_Name + "/pages_" + to_string(i) + ".txt";
      remove(originalFN.c_str());
    } else
      break;
    i++;
  }

  int totalPages = elements / page_chunk_size;

  int totalPasses = ceil(log2f((float)totalPages + 1));

  string pageDirectory = tempFolder_Name + "/pages_";
  for (int pass = 0; pass < totalPasses; pass++) {
    int incrementer = pow(2, pass + 1);
    for (int page = 0; page <= totalPages; page += incrementer) {
      const int FH = page, SH = (page + pow(2, pass)),
                maxSH = (page + incrementer);
      // How many elements are we going to compare(total) in this pass from FH
      // or SH.
      const int elementsCount = pow(2, pass) * page_chunk_size;
      int output_index = FH;

      vector<vector<string> > sortedList;
      int FHptr = FH, SHptr = SH;

      vector<vector<string> > firstHalfData;
      vector<vector<string> > secondHalfData;

      loadPageInMemory(
          pageDirectory + to_string(FHptr++) + "_" + to_string(pass) + ".txt",
          firstHalfData);
      loadPageInMemory(
          pageDirectory + to_string(SHptr++) + "_" + to_string(pass) + ".txt",
          secondHalfData);

      int i = 0, j = 0;
      int s = 0, t = 0;
      while (i < elementsCount && j < elementsCount) {
        if (s >= firstHalfData.size()) {
          firstHalfData.clear();
          s = 0;
          if (FHptr < SH)
            loadPageInMemory(pageDirectory + to_string(FHptr++) + "_" +
                                 to_string(pass) + ".txt",
                             firstHalfData);

          if (firstHalfData.size() == 0) break;
        }
        if (t >= secondHalfData.size()) {
          secondHalfData.clear();
          t = 0;
          if (SHptr < maxSH)
            loadPageInMemory(pageDirectory + to_string(SHptr++) + "_" +
                                 to_string(pass) + ".txt",
                             secondHalfData);

          if (secondHalfData.size() == 0) break;
        }

        if (typeOfSortIndex == "string") {
          if ((attribToOrderBy.second == "ASC" &&
               firstHalfData[s][sortIndex] <= secondHalfData[t][sortIndex]) ||
              (attribToOrderBy.second == "DESC" &&
               firstHalfData[s][sortIndex] > secondHalfData[t][sortIndex])) {
            sortedList.push_back(firstHalfData[s]);
            if (sortedList.size() >= page_chunk_size) {
              ofstream sortedNextPass(pageDirectory +
                                      to_string(output_index++) + "_" +
                                      to_string(pass + 1) + ".txt");
              if (sortedNextPass.is_open()) {
                for (auto& data : sortedList) {
                  sortedNextPass << "{";
                  for (int m = 0; m < data.size(); m++) {
                    sortedNextPass << data[m];
                    if (m + 1 < data.size())
                      sortedNextPass << char(170);
                    else
                      sortedNextPass << "}" << endl;
                  }
                }
                sortedNextPass.close();
              }
              sortedList.clear();
            }
            i++;
            s++;
          } else if ((attribToOrderBy.second == "ASC" &&
                      firstHalfData[s][sortIndex] >
                          secondHalfData[t][sortIndex]) ||
                     (attribToOrderBy.second == "DESC" &&
                      firstHalfData[s][sortIndex] <=
                          secondHalfData[t][sortIndex])) {
            sortedList.push_back(secondHalfData[t]);
            if (sortedList.size() >= page_chunk_size) {
              ofstream sortedNextPass(pageDirectory +
                                      to_string(output_index++) + "_" +
                                      to_string(pass + 1) + ".txt");
              if (sortedNextPass.is_open()) {
                for (auto& data : sortedList) {
                  sortedNextPass << "{";
                  for (int m = 0; m < data.size(); m++) {
                    sortedNextPass << data[m];
                    if (m + 1 < data.size())
                      sortedNextPass << char(170);
                    else
                      sortedNextPass << "}" << endl;
                  }
                }
                sortedNextPass.close();
              }
              sortedList.clear();
            }
            j++;
            t++;
          }
        } else if (typeOfSortIndex == "integer") {
          if ((attribToOrderBy.second == "ASC" &&
               stoi(firstHalfData[s][sortIndex]) <=
                   stoi(secondHalfData[t][sortIndex])) ||
              (attribToOrderBy.second == "DESC" &&
               stoi(firstHalfData[s][sortIndex]) >
                   stoi(secondHalfData[t][sortIndex]))) {
            sortedList.push_back(firstHalfData[s]);
            if (sortedList.size() >= page_chunk_size) {
              ofstream sortedNextPass(pageDirectory +
                                      to_string(output_index++) + "_" +
                                      to_string(pass + 1) + ".txt");
              if (sortedNextPass.is_open()) {
                for (auto& data : sortedList) {
                  sortedNextPass << "{";
                  for (int m = 0; m < data.size(); m++) {
                    sortedNextPass << data[m];
                    if (m + 1 < data.size())
                      sortedNextPass << char(170);
                    else
                      sortedNextPass << "}" << endl;
                  }
                }
                sortedNextPass.close();
              }
              sortedList.clear();
            }
            i++;
            s++;
          } else if ((attribToOrderBy.second == "ASC" &&
                      stoi(firstHalfData[s][sortIndex]) >
                          stoi(secondHalfData[t][sortIndex])) ||
                     (attribToOrderBy.second == "DESC" &&
                      stoi(firstHalfData[s][sortIndex]) <=
                          stoi(secondHalfData[t][sortIndex]))) {
            sortedList.push_back(secondHalfData[t]);
            if (sortedList.size() >= page_chunk_size) {
              ofstream sortedNextPass(pageDirectory +
                                      to_string(output_index++) + "_" +
                                      to_string(pass + 1) + ".txt");
              if (sortedNextPass.is_open()) {
                for (auto& data : sortedList) {
                  sortedNextPass << "{";
                  for (int m = 0; m < data.size(); m++) {
                    sortedNextPass << data[m];
                    if (m + 1 < data.size())
                      sortedNextPass << char(170);
                    else
                      sortedNextPass << "}" << endl;
                  }
                }
                sortedNextPass.close();
              }
              sortedList.clear();
            }
            j++;
            t++;
          }
        } else if (typeOfSortIndex == "float") {
          if ((attribToOrderBy.second == "ASC" &&
               stof(firstHalfData[s][sortIndex]) <=
                   stof(secondHalfData[t][sortIndex])) ||
              (attribToOrderBy.second == "DESC" &&
               stof(firstHalfData[s][sortIndex]) >
                   stof(secondHalfData[t][sortIndex]))) {
            sortedList.push_back(firstHalfData[s]);
            if (sortedList.size() >= page_chunk_size) {
              ofstream sortedNextPass(pageDirectory +
                                      to_string(output_index++) + "_" +
                                      to_string(pass + 1) + ".txt");
              if (sortedNextPass.is_open()) {
                for (auto& data : sortedList) {
                  sortedNextPass << "{";
                  for (int m = 0; m < data.size(); m++) {
                    sortedNextPass << data[m];
                    if (m + 1 < data.size())
                      sortedNextPass << char(170);
                    else
                      sortedNextPass << "}" << endl;
                  }
                }
                sortedNextPass.close();
              }
              sortedList.clear();
            }
            i++;
            s++;
          } else if ((attribToOrderBy.second == "ASC" &&
                      stof(firstHalfData[s][sortIndex]) >
                          stof(secondHalfData[t][sortIndex])) ||
                     (attribToOrderBy.second == "DESC" &&
                      stof(firstHalfData[s][sortIndex]) <=
                          stof(secondHalfData[t][sortIndex]))) {
            sortedList.push_back(secondHalfData[t]);
            if (sortedList.size() >= page_chunk_size) {
              ofstream sortedNextPass(pageDirectory +
                                      to_string(output_index++) + "_" +
                                      to_string(pass + 1) + ".txt");
              if (sortedNextPass.is_open()) {
                for (auto& data : sortedList) {
                  sortedNextPass << "{";
                  for (int m = 0; m < data.size(); m++) {
                    sortedNextPass << data[m];
                    if (m + 1 < data.size())
                      sortedNextPass << char(170);
                    else
                      sortedNextPass << "}" << endl;
                  }
                }
                sortedNextPass.close();
              }
              sortedList.clear();
            }
            j++;
            t++;
          }
        }
      }

      while (i < elementsCount) {
        if (s >= firstHalfData.size()) {
          firstHalfData.clear();
          s = 0;
          if (FHptr < SH)
            loadPageInMemory(pageDirectory + to_string(FHptr++) + "_" +
                                 to_string(pass) + ".txt",
                             firstHalfData);

          if (firstHalfData.size() == 0) break;
        }
        sortedList.push_back(firstHalfData[s]);
        if (sortedList.size() >= page_chunk_size) {
          ofstream sortedNextPass(pageDirectory + to_string(output_index++) +
                                  "_" + to_string(pass + 1) + ".txt");
          if (sortedNextPass.is_open()) {
            for (auto& data : sortedList) {
              sortedNextPass << "{";
              for (int m = 0; m < data.size(); m++) {
                sortedNextPass << data[m];
                if (m + 1 < data.size())
                  sortedNextPass << char(170);
                else
                  sortedNextPass << "}" << endl;
              }
            }
            sortedNextPass.close();
          }
          sortedList.clear();
        }
        s++;
        i++;
      }
      while (j < elementsCount) {
        if (t >= secondHalfData.size()) {
          secondHalfData.clear();
          t = 0;
          if (SHptr < maxSH)
            loadPageInMemory(pageDirectory + to_string(SHptr++) + "_" +
                                 to_string(pass) + ".txt",
                             secondHalfData);

          if (secondHalfData.size() == 0) break;
        }
        sortedList.push_back(secondHalfData[t]);
        if (sortedList.size() >= page_chunk_size) {
          ofstream sortedNextPass(pageDirectory + to_string(output_index++) +
                                  "_" + to_string(pass + 1) + ".txt");
          if (sortedNextPass.is_open()) {
            for (auto& data : sortedList) {
              sortedNextPass << "{";
              for (int m = 0; m < data.size(); m++) {
                sortedNextPass << data[m];
                if (m + 1 < data.size())
                  sortedNextPass << char(170);
                else
                  sortedNextPass << "}" << endl;
              }
            }
            sortedNextPass.close();
          }
          sortedList.clear();
        }
        j++;
        t++;
      }

      if (sortedList.size() > 0) {
        ofstream sortedNextPass(pageDirectory + to_string(output_index++) +
                                "_" + to_string(pass + 1) + ".txt");
        if (sortedNextPass.is_open()) {
          for (auto& data : sortedList) {
            sortedNextPass << "{";
            for (int m = 0; m < data.size(); m++) {
              sortedNextPass << data[m];
              if (m + 1 < data.size())
                sortedNextPass << char(170);
              else
                sortedNextPass << "}" << endl;
            }
          }
          sortedNextPass.close();
        }
        sortedList.clear();
      }
    }
  }

  for (int page = 0; page <= totalPages; page++) {
    vector<vector<string> > PageData;
    loadPageInMemory(
        pageDirectory + to_string(page) + "_" + to_string(totalPasses) + ".txt",
        PageData);
    ofstream output_file(pageDirectory + to_string(page) + ".txt");
    if (output_file.is_open()) {
      for (auto& data : PageData) {
        output_file << "{";
        for (int m = 0; m < data.size(); m++) {
          output_file << data[m];
          if (m + 1 < data.size())
            output_file << char(170);
          else
            output_file << "}" << endl;
        }
      }
    }
  }

  // page_0.txt page_1.txt - sorted
  // First Operation: Merge Page0, page1, then the merged output of these would
  // be merged with page3 and so on.
  //  Merged output of page0 + page1
}

bool group_aggregate_page(string tempFolder_Name, string groupOnAttribName,
                          vector<pair<string, string> > aggregateOnAttribName) {
  unordered_map<string, pair<int, string> > attrib_index_map;
  readTempFolderMeta(tempFolder_Name + "/meta.txt", attrib_index_map);

  int page = 0;
  int outputPage = 0;
  vector<vector<string> > pageData;
  vector<vector<string> > outputData;

  int aggregateSize = aggregateOnAttribName.size();

  string prev_groupOn = "";
  int prev_count = 0;
  vector<float> prev_sum(aggregateSize, 0);
  vector<float> prev_max(aggregateSize, INT_MIN);
  vector<float> prev_min(aggregateSize, INT_MAX);

  int groupOnIndex = attrib_index_map[groupOnAttribName].first;
  vector<int> aggregateIndex;
  for (int i = 0; i < aggregateSize; i++)
    aggregateIndex.push_back(
        attrib_index_map[aggregateOnAttribName[i].first].first);

  while (true) {
    pageData.clear();
    loadPageInMemory(tempFolder_Name + "/pages_" + to_string(page) + ".txt",
                     pageData);
    if (pageData.size() == 0) break;

    for (auto& e : pageData) {
      if (prev_groupOn != e[groupOnIndex]) {
        if (prev_groupOn != "") {
          vector<string> outData;
          outData.push_back(prev_groupOn);
          // Write these data in locations
          for (int i = 0; i < aggregateSize; i++) {
            if (aggregateOnAttribName[i].second == "MAX") {
              if (attrib_index_map[aggregateOnAttribName[i].first].second ==
                  "integer")
                outData.push_back(to_string((int)prev_max[i]));
              else if (attrib_index_map[aggregateOnAttribName[i].first]
                           .second == "float")
                outData.push_back(to_string(prev_max[i]));
            } else if (aggregateOnAttribName[i].second == "MIN") {
              if (attrib_index_map[aggregateOnAttribName[i].first].second ==
                  "integer")
                outData.push_back(to_string((int)prev_min[i]));
              else if (attrib_index_map[aggregateOnAttribName[i].first]
                           .second == "float")
                outData.push_back(to_string(prev_min[i]));
            } else if (aggregateOnAttribName[i].second == "CNT") {
              outData.push_back(to_string(prev_count));
            } else if (aggregateOnAttribName[i].second == "SUM") {
              if (attrib_index_map[aggregateOnAttribName[i].first].second ==
                  "integer")
                outData.push_back(to_string((int)prev_sum[i]));
              else if (attrib_index_map[aggregateOnAttribName[i].first]
                           .second == "float")
                outData.push_back(to_string(prev_sum[i]));
            } else if (aggregateOnAttribName[i].second == "AVG")
              outData.push_back(to_string(prev_sum[i] / prev_count));
          }

          outputData.push_back(outData);
          if (outputData.size() >= page_chunk_size) {
            ofstream pageWrite(tempFolder_Name + "/pages_" +
                               to_string(outputPage++) + ".txt");
            if (pageWrite.is_open()) {
              for (auto& e : outputData) {
                pageWrite << "{";
                for (int k = 0; k < e.size(); k++) {
                  pageWrite << e[k];
                  if (k + 1 < e.size())
                    pageWrite << char(170);
                  else
                    pageWrite << "}" << endl;
                }
              }
              pageWrite.close();
            }
            outputData.clear();
          }
        }
        // set to new group on attribute
        prev_groupOn = e[groupOnIndex];
        prev_count = 1;

        prev_sum.clear();
        prev_sum.resize(aggregateSize, 0);

        prev_max.clear();
        prev_max.resize(aggregateSize, INT_MIN);

        prev_min.clear();
        prev_min.resize(aggregateSize, INT_MAX);
        for (int i = 0; i < aggregateSize; i++) {
          // has a decimal point, is float, else will always be integer

          if (attrib_index_map[aggregateOnAttribName[i].first].second ==
              "float")
          // if (find(e[aggregateIndex[i]].begin(), e[aggregateIndex[i]].end(),
          // '.') != e[aggregateIndex[i]].end())
          {
            prev_sum[i] += stof(e[aggregateIndex[i]]);
            prev_max[i] = max(prev_max[i], stof(e[aggregateIndex[i]]));
            prev_min[i] = min(prev_max[i], stof(e[aggregateIndex[i]]));
          } else if (attrib_index_map[aggregateOnAttribName[i].first].second ==
                     "integer")
          // else if (e[aggregateIndex[i]][0] <= '9' && e[aggregateIndex[i]][0]
          // >= '0')
          {
            prev_sum[i] += stoi(e[aggregateIndex[i]]);
            prev_max[i] = max((int)prev_max[i], stoi(e[aggregateIndex[i]]));
            prev_min[i] = min((int)prev_max[i], stoi(e[aggregateIndex[i]]));
          }
        }
      } else {
        prev_count++;
        for (int i = 0; i < aggregateSize; i++) {
          // has a decimal point, is float, else will always be integer
          if (attrib_index_map[aggregateOnAttribName[i].first].second ==
              "float")
          // if (find(e[aggregateIndex[i]].begin(), e[aggregateIndex[i]].end(),
          // '.') != e[aggregateIndex[i]].end())
          {
            prev_sum[i] += stof(e[aggregateIndex[i]]);
            prev_max[i] = max(prev_max[i], stof(e[aggregateIndex[i]]));
            prev_min[i] = min(prev_min[i], stof(e[aggregateIndex[i]]));
          } else if (attrib_index_map[aggregateOnAttribName[i].first].second ==
                     "integer")
          // else if (e[aggregateIndex[i]][0] <= '9' && e[aggregateIndex[i]][0]
          // >= '0')
          {
            prev_sum[i] += stoi(e[aggregateIndex[i]]);
            prev_max[i] = max((int)prev_max[i], stoi(e[aggregateIndex[i]]));
            prev_min[i] = min((int)prev_min[i], stoi(e[aggregateIndex[i]]));
          }
        }
      }
    }

    page++;
  }

  if (prev_groupOn != "") {
    vector<string> outData;
    outData.push_back(prev_groupOn);
    // Write these data in locations
    for (int i = 0; i < aggregateSize; i++) {
      if (aggregateOnAttribName[i].second == "MAX") {
        if (attrib_index_map[aggregateOnAttribName[i].first].second ==
            "integer")
          outData.push_back(to_string((int)prev_max[i]));
        else if (attrib_index_map[aggregateOnAttribName[i].first].second ==
                 "float")
          outData.push_back(to_string(prev_max[i]));
      } else if (aggregateOnAttribName[i].second == "MIN") {
        if (attrib_index_map[aggregateOnAttribName[i].first].second ==
            "integer")
          outData.push_back(to_string((int)prev_min[i]));
        else if (attrib_index_map[aggregateOnAttribName[i].first].second ==
                 "float")
          outData.push_back(to_string(prev_min[i]));
      } else if (aggregateOnAttribName[i].second == "CNT") {
        outData.push_back(to_string(prev_count));
      } else if (aggregateOnAttribName[i].second == "SUM") {
        if (attrib_index_map[aggregateOnAttribName[i].first].second ==
            "integer")
          outData.push_back(to_string((int)prev_sum[i]));
        else if (attrib_index_map[aggregateOnAttribName[i].first].second ==
                 "float")
          outData.push_back(to_string(prev_sum[i]));
      } else if (aggregateOnAttribName[i].second == "AVG")
        outData.push_back(to_string(prev_sum[i] / prev_count));
    }

    outputData.push_back(outData);
    if (outputData.size() >= page_chunk_size) {
      ofstream pageWrite(tempFolder_Name + "/pages_" + to_string(outputPage++) +
                         ".txt");
      if (pageWrite.is_open()) {
        for (auto& e : outputData) {
          pageWrite << "{";
          for (int k = 0; k < e.size(); k++) {
            pageWrite << e[k];
            if (k + 1 < e.size())
              pageWrite << char(170);
            else
              pageWrite << "}" << endl;
          }
        }
        pageWrite.close();
      }
      outputData.clear();
    }
  }
  if (outputData.size() > 0) {
    ofstream pageWrite(tempFolder_Name + "/pages_" + to_string(outputPage++) +
                       ".txt");
    if (pageWrite.is_open()) {
      for (auto& e : outputData) {
        pageWrite << "{";
        for (int k = 0; k < e.size(); k++) {
          pageWrite << e[k];
          if (k + 1 < e.size())
            pageWrite << char(170);
          else
            pageWrite << "}" << endl;
        }
      }
      pageWrite.close();
    }
    outputData.clear();
  }

  ofstream groupAggregateMeta(tempFolder_Name + "/meta.txt");
  if (groupAggregateMeta.is_open()) {
    groupAggregateMeta << 0 << char(170) << groupOnAttribName << char(170)
                       << attrib_index_map[groupOnAttribName].second << endl;
    for (int i = 0; i < aggregateSize; i++) {
      groupAggregateMeta << i + 1 << char(170)
                         << aggregateOnAttribName[i].second << "("
                         << aggregateOnAttribName[i].first << ")" << char(170);
      if (aggregateOnAttribName[i].second == "CNT")
        groupAggregateMeta << "integer" << endl;
      else if (aggregateOnAttribName[i].second == "AVG")
        groupAggregateMeta << "float" << endl;
      else
        groupAggregateMeta
            << attrib_index_map[aggregateOnAttribName[i].first].second << endl;
    }
  }

  return true;
}