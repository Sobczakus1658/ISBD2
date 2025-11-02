
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include "codec.h"
#include <unordered_map>

using namespace std;

inline constexpr std::size_t BATCH_SIZE = 8192;
inline constexpr uint32_t file_magic = 0x21374201;
inline constexpr uint32_t batch_magic = 0x69696969;

struct Batch {
    std::vector<IntColumn> intColumns;
    std::vector<StringColumn> stringColumns;
    size_t num_rows;
};

// struct EncodeIntColumn {
//     std::string name;
//     std::vector<uint8_t> compressed_data;
//     uint64_t delta_base;
// };

// struct EncodeStringColumn {
//     std::string name;
//     std::vector<uint8_t> compressed_data;
//     uint32_t uncompressed_size;  
//     uint32_t compressed_size;
// };

// void variableLengthEncoding(std::vector<uint8_t> & compressed_data, std::vector<uint64_t>& column) {
//     bool flag = true;
//     for (auto value : column) {
//         flag = true;
//         while(flag) {
//             uint8_t byte = value & 0x7F;  
//             value >>= 7;
//             if (byte == 0) {
//                 compressed_data.push_back(byte);
//                 flag = false;
//             } else {
//                 byte |= 0x80;
//                 compressed_data.push_back(byte);
//             }
//         }
//     }
// }

// void variableLengthDecoding(std::vector<uint8_t> & compressed_data, std::vector<uint64_t>& column) {
//     uint64_t value = 0;
//     uint8_t shift = 0;

//     for (uint8_t chunk : compressed_data) {
//         value |= (uint64_t)(chunk & 0x7F) << shift;
//         if ((chunk & 0x80) == 0) {
//             column.push_back(value);
//             value = 0;
//             shift = 0;
//         } else {
//             shift += 7;
//         }
//     }
// }

// void deltaDecoding(std::vector<uint64_t>& column, uint64_t base) {
//     if (column.empty()) return;

//     uint64_t size =column.size();
//     for (uint64_t i = 1; i < size; i++) {
//         column.at(i) += base;
//     }
// }

// void deltaEncoding(std::vector<uint64_t>& column, uint64_t base) {
//     if (column.empty()) return ;
//     uint64_t size = column.size();

//     for (uint64_t i = 1; i < size; i++){
//         column.at(i) -= base;
//     }
// }

// EncodeIntColumn encodeSingleIntColumn(IntColumn& column) {
//     EncodeIntColumn out;
//     out.name = column.name;
//     if (column.column.empty()) {
//         out.delta_base = 0;
//         out.compressed_data.clear();
//         return out;
//     }
//     auto min_it = std::min_element(column.column.begin(), column.column.end());
//     out.delta_base = *min_it;
//     deltaEncoding(column.column, out.delta_base);
//     variableLengthEncoding(out.compressed_data, column.column);
//     return out;
// }

// IntColumn decodeSingleIntColumn(EncodeIntColumn& column) {
//     IntColumn out;
//     out.name = column.name;
//     variableLengthDecoding(column.compressed_data, out.column);
//     deltaDecoding(out.column, column.delta_base);
//     return out;
// }

// EncodeStringColumn* encodeSingleStringColumn(StringColumn& column) {
//     auto* out = new EncodeStringColumn();

//     size_t total_size = 0;
//     for (const auto &s : column.column) {
//         total_size += s.size() + 1; 
//     }

//     std::string blob;
//     blob.reserve(total_size);
//     for (const auto &val : column.column) {
//         blob.append(val);
//         blob.push_back('\0');
//     }

//     out->name = std::move(column.name);

//     size_t uncompressed_size = blob.size();
//     out->uncompressed_size = static_cast<uint32_t>(uncompressed_size);

//     size_t bound = ZSTD_compressBound(uncompressed_size);
//     out->compressed_data.resize(bound);

//     size_t compressed_size = ZSTD_compress(
//         out->compressed_data.data(),
//         out->compressed_data.size(),
//         blob.data(),
//         uncompressed_size,
//         compresion_level
//     );

//     if (ZSTD_isError(compressed_size)) {
//         std::cerr << "ZSTD compression error: " << ZSTD_getErrorName(compressed_size) << "\n";
//         delete out;
//         return nullptr;
//     }

//     out->compressed_data.resize(compressed_size);
//     out->compressed_size = static_cast<uint32_t>(compressed_size);

//     return out;
// }

// StringColumn decodeSingleStringColumn(EncodeStringColumn& column) {

//     StringColumn out;
//     out.name = std::move(column.name);

//     std::string decompressed(column.uncompressed_size, '\0');
//     ZSTD_decompress(
//         decompressed.data(),
//         column.uncompressed_size,
//         column.compressed_data.data(),
//         column.compressed_size
//     );

//     size_t start = 0;
//     for (size_t i = 0; i < decompressed.size(); ++i) {
//         if (decompressed[i] == '\0') {
//             out.column.emplace_back(decompressed.substr(start, i - start));
//             start = i + 1;
//         }
//     }
//     return out;
// }

// void decodeStringColumn(std::vector<StringColumn>* columns, std::ifstream& in) {
//     EncodeStringColumn column;

//     std::string name;
//     uint32_t name_len;
//     uint32_t uncompressed_size;
//     uint32_t compressed_size;

//     in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
//     name.resize(name_len);
//     if (name_len > 0) {
//         in.read(&name[0], name_len);
//     }
//     column.name = std::move(name);

//     in.read(reinterpret_cast<char*>(&uncompressed_size), sizeof(uncompressed_size));
//     in.read(reinterpret_cast<char*>(&compressed_size), sizeof(compressed_size));
//     column.uncompressed_size = uncompressed_size;
//     column.compressed_size = compressed_size;
//     column.compressed_data.resize(static_cast<size_t>(compressed_size));
//     if (compressed_size > 0) {
//         in.read(reinterpret_cast<char*>(column.compressed_data.data()), compressed_size);
//     }

//     columns->push_back(std::move(decodeSingleStringColumn(column)));
// }

// void encodeMetaDataInt(std::ofstream& out, IntColumn& column){
//     EncodeIntColumn col = encodeSingleIntColumn(column);
//     uint32_t len = col.name.size();
//     uint32_t compressed_bits_length = col.compressed_data.size();
//     uint64_t delta_base = col.delta_base;

//     out.write((char*)&len, sizeof(len));
//     out.write((char*)col.name.data(), len);

//     out.write((char*)&delta_base, sizeof(delta_base));

//     out.write((char*)&compressed_bits_length, sizeof(compressed_bits_length));
//     if (compressed_bits_length > 0) {
//         out.write((char*)(col.compressed_data.data()), compressed_bits_length);
//     }
// }

// void encodeMetaDataString(std::ofstream& out, StringColumn& column){
//     EncodeStringColumn* col = encodeSingleStringColumn(column);
//     uint32_t len = (*col).name.size();
//     uint32_t uncompressed_size = (*col).uncompressed_size;
//     uint32_t compressed_size = (*col).compressed_size;

//     out.write((char*)&len, sizeof(len));
//     out.write((char*)(*col).name.data(), len);

//     out.write((char*)&uncompressed_size, sizeof(uncompressed_size));
//     out.write((char*)&compressed_size , sizeof(compressed_size));

//     out.write((char*)(*col).compressed_data.data(), compressed_size);
// }

void serializator(std::vector<Batch> &batches, const std::string& filepath) {
    std::ofstream out(filepath, std::ios::binary);
    uint32_t batches_len = batches.size();

    if(!out) {
        std::cerr << "serializator: cannot open file " << filepath << "\n";
        return;
    }

    out.write((char*)&file_magic, sizeof(file_magic));
    out.write((char*)&batches_len, sizeof(batches_len));

    for (auto &batch : batches) {
        uint32_t int_len = batch.intColumns.size();
        uint32_t string_len = batch.stringColumns.size();

    out.write((char*)&batch_magic, sizeof(batch_magic));
    uint32_t batch_num_rows32 = static_cast<uint32_t>(batch.num_rows);
    out.write(reinterpret_cast<char*>(&batch_num_rows32), sizeof(batch_num_rows32));

        out.write((char*)&int_len, sizeof(int_len));
        out.write((char*)&string_len, sizeof(string_len));

        for(auto intColumn : batch.intColumns) {
            encodeMetaDataInt(out, intColumn);
        }

        for(auto stringColumn : batch.stringColumns) {
            encodeMetaDataString(out, stringColumn);
        }
    }
}

// void decodeIntColumn(std::vector<IntColumn>* columns, std::ifstream& in) {
//     EncodeIntColumn column;

//     std::string name;
//     uint32_t name_len;
//     uint32_t compressed_bits_length;
//     uint64_t delta_base;

//     in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
//     name.resize(name_len);
//     if (name_len > 0) {
//         in.read(&name[0], name_len);
//     }
//     column.name = std::move(name);

//     in.read(reinterpret_cast<char*>(&delta_base), sizeof(delta_base));
//     column.delta_base = delta_base;

//     in.read(reinterpret_cast<char*>(&compressed_bits_length), sizeof(compressed_bits_length));
//     column.compressed_data.resize(static_cast<size_t>(compressed_bits_length));
//     if (compressed_bits_length > 0) {
//         in.read(reinterpret_cast<char*>(column.compressed_data.data()), compressed_bits_length);
//     }

//     columns->push_back(std::move(decodeSingleIntColumn(column)));
// }

Batch deserializatorBatch(std::ifstream& in, const std::string& filepath){
    uint32_t read_batch_magic;
    uint32_t batch_num_rows;
    uint32_t int_len;
    uint32_t string_len;

    in.read((char *)&read_batch_magic, sizeof(read_batch_magic));
    if(read_batch_magic != batch_magic){
        std::cerr << "Invalid batch_magic";
        return Batch();
    }
    in.read((char *)&batch_num_rows, sizeof(batch_num_rows));
    in.read((char *)&int_len, sizeof(int_len));
    in.read((char *)&string_len, sizeof(string_len));

    Batch batch;
    batch.num_rows = batch_num_rows;
    for (uint32_t j = 0; j < int_len; j++) {
        decodeIntColumn(&batch.intColumns, in);
    }
    for (uint32_t j = 0; j < string_len; j++) {
        decodeStringColumn(&batch.stringColumns, in);
    }
    return batch;
}

std::vector<Batch> deserializator(const std::string& filepath) {
    std::vector<Batch> batches;
    std::ifstream in(filepath, std::ios::binary);
    if (!in) {
        std::cerr << "deserializator: cannot open file " << filepath << "\n";
        return {};
    }
    uint32_t read_file_magic;
    in.read((char *)&read_file_magic, sizeof(read_file_magic));
    if(read_file_magic != file_magic){
        std::cerr << "Invalid file_magic";
        return {};
    }
    uint32_t batches_len;
    in.read((char *)&batches_len, sizeof(batches_len));

    for (uint32_t i = 0; i < batches_len; i++) {
        batches.push_back(deserializatorBatch(in, filepath));
    }
    return batches;
}

std::vector<Batch> createSampleBatches(){
    std::vector<Batch> batches;
    
    Batch a;
    a.num_rows = 3;
    IntColumn idCol;
    idCol.name = "id"; 
    idCol.column = {1, 2, 3};
    IntColumn ageCol;
    ageCol.name = "wiek"; 
    ageCol.column = {67, 68, 69};
    StringColumn nameCol; 
    nameCol.name = "imie"; 
    nameCol.column = {"Zbyszek", "Halina", "Onufry"};
    a.intColumns.push_back(std::move(idCol));
    a.intColumns.push_back(std::move(ageCol));
    a.stringColumns.push_back(std::move(nameCol));
    batches.push_back(std::move(a));
    
    Batch b;
    b.num_rows = 4;
    IntColumn population; 
    population.name = "populacja"; 
    population.column = {24892, 51234, 1236, 712};
    StringColumn cityCol;
    cityCol.name = "miasto"; 
    cityCol.column = {"Skierniewice", "Sochaczew", "Nowa Sucha", "Kozlow Biskupi"};
    StringColumn wies;
    wies.name = "czy wies"; 
    wies.column = {"nie", "nie", "tak", "tak"};
    b.intColumns.push_back(std::move(population));
    b.stringColumns.push_back(std::move(cityCol));
    b.stringColumns.push_back(std::move(wies));
    batches.push_back(std::move(b));
    

    Batch c;
    c.num_rows = 2;
    IntColumn feetSize; 
    feetSize.name = "rozmiar stopy"; 
    feetSize.column = {44, 44};
    StringColumn colorShoe;
    colorShoe.name = "kolor buta"; 
    colorShoe.column = {"zolty", "zielony"};
    c.intColumns.push_back(std::move(feetSize));
    c.stringColumns.push_back(std::move(colorShoe));
    batches.push_back(std::move(c));

    return batches;
}

void calculateMean(IntColumn& column){
    __int128_t sum = 0;
    for(auto el : column.column) {
        sum += el;
    }
    double mean = static_cast<double>(sum) / static_cast<double>(column.column.size());
    std::cout << "Mean for column " << column.name << " is " << mean << "\n";
}

void calculateSum(StringColumn& column) {
    std::unordered_map<char, int> ascii_count;

    for (const auto &s : column.column) {
        for (unsigned char uc : s) {
            char c = static_cast<char>(uc);
            ascii_count[c]++;
        }
    }

    std::cout << "Character frequency analysis for column " << column.name << " \n";
    for (const auto &kv : ascii_count) {
        char ch = kv.first;
        int count = kv.second;
        if (ch == '\n') {
            std::cout << "'\\n' -> " << count << "\n";
        } else {
            std::cout << "'" << ch << "'" << "-> " << count << "\n";
        }
    }
}

void calculateStatistics(std::vector<Batch> &batches) {
    for (auto &batch : batches) {
        for(auto &column : batch.intColumns) {
            calculateMean(column);
        }
        for(auto &column : batch.stringColumns) {
            calculateSum(column);
        }
    }
}

void checkIntColumn(IntColumn& first, IntColumn& second){
    if (first.name != second.name) {
        std::cerr<<"Expected name: " << first.name << " but received: " <<  "second.name \n";  
    }

    if (first.column.size() != second.column.size()) {
        std::cerr<<"Column " << first.name << " has different number of rows\n";
    }

    if (first.column != second.column) {
        std::cerr<<"Column " << first.name << " has different values \n";
    }
}

void checkStringColumn(StringColumn& first, StringColumn& second){
    if (first.name != second.name) {
        std::cerr<<"Expected name: " << first.name << " but received: " <<  "second.name \n";  
    }

    if (first.column.size() != second.column.size()) {
        std::cerr<<"Column " << first.name << " has different number of rows\n";
    }

    if (first.column != second.column) {
        std::cerr<<"Column " << first.name << " has different values \n";
    }
}

void checkEqualBatches(Batch& first, Batch& second) {
    if (first.num_rows != second.num_rows){
        std::cerr<<"Batches have different number of rows \n";
        return;
    }

    if (first.intColumns.size() != second.intColumns.size()) {
        std::cerr<<"Batches have different number int columns \n";
    }

    if (first.stringColumns.size() != second.stringColumns.size()) {
        std::cerr<<"Batches have different number string columns \n";
    }

    uint32_t size = first.intColumns.size();
    for(uint32_t i = 0; i < size; i++) {
        checkIntColumn(first.intColumns.at(i), second.intColumns.at(i));
    }

    size = first.stringColumns.size();
    for(uint32_t i = 0; i < size; i++) {
        checkStringColumn(first.stringColumns.at(i), second.stringColumns.at(i));
    }

}

void workflow(const std::string& filepath) {

    std::vector<Batch> batches = std::move(createSampleBatches());
    serializator(batches, filepath);
    std::vector<Batch> deserializated_batches = std::move(deserializator(filepath));
    calculateStatistics(deserializated_batches);

    uint32_t size = batches.size();
    for(uint32_t i = 0; i < size; i++) {
        checkEqualBatches(batches.at(i), deserializated_batches.at(i));
    }
}

int main(){
    const std::string& filepath = "/home/sobczakus/implementacja/ISBD2/batches.bin";
    workflow(filepath);
}
