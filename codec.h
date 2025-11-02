#include <vector>
#include <string>
#include <iostream>
#include <fstream>

struct IntColumn {
    std::string name;
    std::vector<uint64_t> column;
};

struct StringColumn {
    std::string name;
    std::vector<std::string> column;
};
void encodeMetaDataString(std::ofstream& out, StringColumn& column);

void encodeMetaDataInt(std::ofstream& out, IntColumn& column);

void decodeIntColumn(std::vector<IntColumn>* columns, std::ifstream& in); 

void decodeStringColumn(std::vector<StringColumn>* columns, std::ifstream& in);