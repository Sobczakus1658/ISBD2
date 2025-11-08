#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <unordered_map>

#include "../types.h"

uint64_t encodeIntColumns(std::ofstream& out, std::vector<IntColumn>& columns, std::unordered_map<std::string, uint64_t>& map);

uint64_t encodeMetaDataInt(std::ofstream& out, IntColumn& column);

void decodeIntColumns(std::ifstream& in, std::vector<IntColumn>& columns, uint32_t length); 

std::pair<uint64_t, IntColumn> decodeIntColumnBlock(std::ifstream& in);
