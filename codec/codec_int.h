#include <vector>
#include <string>
#include <iostream>
#include <fstream>

#include "../types.h"

void encodeIntColumns(std::ofstream& out, std::vector<IntColumn>& columns);

void decodeIntColumns(std::ifstream& in, std::vector<IntColumn>& columns, uint32_t length); 
