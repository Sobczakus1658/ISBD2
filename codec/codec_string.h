#include <vector>
#include <string>
#include <iostream>
#include <fstream>

#include "../types.h"

inline constexpr int compresion_level = 3;


void encodeStringColumns(std::ofstream& out, std::vector<StringColumn>& columns);

void decodeStringColumns(std::ifstream& in, std::vector<StringColumn>& columns, uint32_t length); 