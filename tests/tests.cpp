#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "../validation/validator.h"
#include "../serialization/serializator.h"
#include "../serialization/deserializator.h"
#include "../statistics/statistics.h"

static constexpr uint64_t PART_LIMIT = 3500ULL * 1024ULL * 1024ULL;
static constexpr uint64_t SHORTER_LIMIT = 3500ULL * 1024ULL;
namespace fs = std::filesystem;
static const std::string base =  fs::current_path() / "batches/";

std::vector<Batch> createBatchesForColumnTest(){
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
    nameCol.column = {"Zbyszek", "Halina", "Grażyna"};
    a.intColumns.push_back(std::move(idCol));
    a.intColumns.push_back(std::move(ageCol));
    a.stringColumns.push_back(std::move(nameCol));
    batches.push_back(std::move(a));

    Batch b;
    b.num_rows = 2;
    idCol.column = {1, 2};
    ageCol.column = {67, 68};
    nameCol.name = "imie"; 
    nameCol.column = {"Stefan", "Alojzy"};
    b.intColumns.push_back(std::move(idCol));
    b.intColumns.push_back(std::move(ageCol));
    b.stringColumns.push_back(std::move(nameCol));
    batches.push_back(std::move(b));

    Batch c;
    c.num_rows = 4;
    idCol.column = {1, 2, 3, 5};
    ageCol.name = "wiek"; 
    ageCol.column = {67, 68, 69, 70};
    nameCol.name = "imie"; 
    nameCol.column = {"Benifacy", "Bronisław", "Gerwazy", "Filip"};
    c.intColumns.push_back(std::move(idCol));
    c.intColumns.push_back(std::move(ageCol));
    c.stringColumns.push_back(std::move(nameCol));
    batches.push_back(std::move(c));
    
    return batches;
}

std::vector<Batch> createBatchesForSimpleTest(){
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
    nameCol.column = {"Zbyszek", "Halina", "Grażyna"};
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

std::vector<Batch> createBigSampleBatches(){
    std::vector<Batch> batches;

    const uint64_t TARGET_BYTES = PART_LIMIT; 

    const size_t rows_per_batch = 65536; 
    const size_t str_len = 200;
    const size_t string_columns = 2;
    const size_t int_columns = 2;

    const uint64_t per_batch_est = rows_per_batch * (
        string_columns * (str_len + 1) + 
        int_columns * sizeof(uint64_t)
    );

    uint64_t accumulated = 0;
    size_t batch_idx = 0;

    std::string base_str(str_len, 'x');
    for (size_t i = 0; i < str_len; ++i) base_str[i] = static_cast<char>('a' + (i % 26));

    size_t reserve_batches = static_cast<size_t>((TARGET_BYTES / std::max<uint64_t>(1, per_batch_est)) + 4);
    batches.reserve(std::min<size_t>(reserve_batches, 100000));

    while (accumulated < TARGET_BYTES) {
        Batch b;
        b.num_rows = rows_per_batch;

        for (size_t ic = 0; ic < int_columns; ++ic) {
            IntColumn col;
            col.name = "big_int_" + std::to_string(ic);
            col.column.resize(rows_per_batch);
            for (size_t r = 0; r < rows_per_batch; ++r) col.column[r] = static_cast<uint64_t>(r + batch_idx);
            b.intColumns.push_back(std::move(col));
        }

        for (size_t sc = 0; sc < string_columns; ++sc) {
            StringColumn col;
            col.name = "big_str_" + std::to_string(sc);
            col.column.resize(rows_per_batch);
            for (size_t r = 0; r < rows_per_batch; ++r) {
                col.column[r] = base_str + std::to_string((batch_idx + r) % 1000);
            }
            b.stringColumns.push_back(std::move(col));
        }

        batches.push_back(std::move(b));

        accumulated += per_batch_est;
        ++batch_idx;

    }

    return batches;
}

void columnTest(){
    std::cout<< "Running Column test ... \n";
    std::string folderPath = base + "columnTest";
    std::string filePath = folderPath + ".part000";

    std::vector<Batch> batches = std::move(createBatchesForColumnTest());
    serializator(batches, folderPath, 3500ULL * 1024ULL * 1024ULL);

    std::vector<Batch> bats = readColumn(filePath, "imie");
    std::vector<std::string> values;
    std::vector<std::string> expectedValues = {"Zbyszek", "Halina", "Grażyna", "Stefan", "Alojzy", "Benifacy", "Bronisław", "Gerwazy", "Filip"};

    if (bats.empty()) {
        std::cerr << "columnTest: readColumn returned no batches\n";
        return;
    }

    std::string column_name;
    if (!bats.front().stringColumns.empty()) {
        column_name = bats.front().stringColumns.front().name;
    } else {
        std::cerr << "columnTest: first batch has no string columns\n";
        return;
    }

    for (const auto &b : bats) {
        for (const auto &sc : b.stringColumns) {
            assert(sc.name == column_name);
            values.insert(values.end(), sc.column.begin(), sc.column.end());
        }
    }

    if (column_name != "imie") {
        std::cerr << "expected to read imie but got" << column_name << "\n";
        return;
    }

    if (values != expectedValues) {
        std::cerr << "Expected values and read values are different \n";
        return;
    }
    std::cout<< "Column Test Passed \n \n";
}

void someFilesTest(){
    std::cout<< "Running Files test ... \n";
    std::string folderPath = base + "someFiles";

    std::cout<< "Trying to create Lots of batches ... \n";
    std::vector<Batch> batches = std::move(createBigSampleBatches());
    std::cout<< "Batches created succesfully \n";

    std::cout<< "Starting seralization ... \n";
    serializator(batches, folderPath, SHORTER_LIMIT);
    std::cout<< "Seralization finished \n";

    fs::path dir = fs::path(folderPath).parent_path();
    std::string base = fs::path(folderPath).filename().string();
    std::string prefix = base + ".part";

    std::size_t fileCount = 0;
    for (auto const& entry : fs::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        const std::string fname = entry.path().filename().string();
        if (fname.rfind(prefix, 0) != 0) continue; 

        ++fileCount;

        std::ifstream in(entry.path(), std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "someFilesTest: cannot open " << entry.path() << "\n";
            assert(false);
        }

        uint32_t magic = 0;
        in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        if (!in || in.gcount() != static_cast<std::streamsize>(sizeof(magic))) {
            std::cerr << "someFilesTest: file too small " << entry.path() << "\n";
            assert(false);
        }

        if (magic != file_magic) {
            std::cerr << "someFilesTest: bad magic in " << entry.path() << ": 0x" << std::hex << magic << std::dec << "\n";
            assert(false);
        }
    }

    if (fileCount <= 1) {
        std::cerr << "someFilesTest: expected more than one part file with prefix '" << prefix << "' in " << dir << "\n";
        assert(false);
    }

    std::cout << "Found " << fileCount << " part files with valid file magic\n";
    std::cout<< "Some files Test Passed \n \n";
}

void simpleTest(){
    std::cout<< "Running simple test ... \n";
    std::string folderPath = base + "simple";
    std::string filePath = folderPath + ".part000";

    std::vector<Batch> batches = std::move(createBatchesForSimpleTest());
    serializator(batches, folderPath, PART_LIMIT);
    std::vector<Batch> deserializated_batches = std::move(deserializator(filePath));
    validateBatches(batches, deserializated_batches);

    std::cout<<"Batches before serialization and after deserialization are the same \n";
    std::cout<<"There are expected statistics \n";
    calculateStatistics(deserializated_batches);
    std::cout<< "Simple Test Passed \n \n";
}

void bigTest(){
    std::string folderPath = base + "bigTest";
    std::string filePath = folderPath + ".part000";

    std::cout<< "Running big test ... \n";

    std::cout<< "Trying to create Lots of batches ... \n";
    std::vector<Batch> batches = std::move(createBigSampleBatches());
    std::cout<< "Batches created succesfully \n";

    std::cout<< "Starting seralization ... \n";
    serializator(batches, folderPath, PART_LIMIT);
    std::cout<< "Seralization finished \n";

    std::vector<Batch> deserializated_batches = std::move(deserializator(filePath));
    validateBatches(batches, deserializated_batches);
    std::cout<< "Big Test Passed \n \n";
}

void clearAfterTests() {
    namespace fs = std::filesystem;
    fs::path dir = base;
    try {
        if (!fs::exists(dir)) {
            std::cerr << "clearAfterTest: directory does not exist: " << dir << "\n";
            return;
        }
        for (auto const& entry : fs::directory_iterator(dir)) {
            try {
                fs::remove_all(entry.path());
            } catch (const std::exception &e) {
                std::cerr << "clearAfterTest: failed to remove " << entry.path() << ": " << e.what() << "\n";
            }
        }
        std::cout << "clearAfterTest: cleared contents of " << dir << "\n";
    } catch (const std::exception &e) {
        std::cerr << "clearAfterTest: error while clearing " << dir << ": " << e.what() << "\n";
    }
}