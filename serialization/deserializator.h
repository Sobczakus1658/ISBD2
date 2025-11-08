#include "../types.h"
#include <string>

std::vector<Batch> deserializator(const std::string& filepath);

std::vector<Batch> readColumn(const std::string& filepath, std::string column);
