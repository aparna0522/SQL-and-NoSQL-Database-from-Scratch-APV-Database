/*
Common tweakable parameters to change how much data is stored in files (BTree nodes or temp processing pages)
*/
#pragma once
#include <string>

// Should only be even, this defines number of children a BTreeNode will have
#define ORDER 20
#define CHUNK_SIZE 50
#define PRINT_ROWS INT_MAX