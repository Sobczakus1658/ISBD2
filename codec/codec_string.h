#include <vector>
#include <string>
#include <iostream>
#include <fstream>

#include "../types.h"

inline constexpr int compresion_level = 3;


void encodeStringColumns(ofstream& out, vector<StringColumn>& columns);

uint64_t encodeSingleStringColumn(ofstream& out, StringColumn& column);

void decodeStringColumns(ifstream& in, vector<StringColumn>& columns, uint32_t length); 

pair<uint64_t, StringColumn> decodeStringColumnBlock(ifstream& in);