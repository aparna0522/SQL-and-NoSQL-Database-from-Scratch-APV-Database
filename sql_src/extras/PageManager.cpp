/*
Compile this .cpp file to be used as a dependency for no-sql queries
Provides an API interface for python to execute temp folder processings: ordering, joining and grouping_aggregation.
*/

#include "../pages.h"
#include <iostream>

using namespace std;

int main(int argc, char* argv[])
{
   if (argc < 2)
       return -1;

   string t = argv[1];
   if (t == "order")
   {
       string tempFolder = string(argv[2]);
       string attribName = string(argv[3]);
       string order = string(argv[4]);
       page_chunk_size = stoi(string(argv[5]));

       if (order_page(tempFolder, { attribName, order }))
           return 0;
       else
           return 1;
   }

   else if (t == "join")
   {
       string table1Folder = string(argv[2]);
       string table2Folder = string(argv[3]);

       string onClause_1 = string(argv[4]);
       string onClause_2 = string(argv[5]);
       string onClause_3 = string(argv[6]);

       string outFolder = string(argv[7]);
       page_chunk_size = stoi(string(argv[8]));

       if (join_page(table1Folder, table2Folder, {onClause_1, onClause_2, onClause_3}, outFolder))
           return 0;
       else
           return 1;
   }

   else if (t == "grp_agg")
   {
       string tempFolder = string(argv[2]);
       string attribName = string(argv[3]);
       vector<pair<string, string>> AggragateOnAttribName;
       for (int i = 4; i < argc - 1; i+=2)
       {
           string aggregateName = string(argv[i]);
           string aggregateOperation = string(argv[i+1]);
           AggragateOnAttribName.push_back({ aggregateName, aggregateOperation });
       }

       page_chunk_size = stoi(string(argv[argc-1]));

       if(group_aggregate_page(tempFolder, attribName, AggragateOnAttribName))
           return 0;
       else
           return 1;
   }

   return 0;
}