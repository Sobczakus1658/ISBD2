#include <iostream>
#include <fstream>
#include <unordered_map>

#include "validation/validator.h"
#include "serialization/serializator.h"
#include "serialization/deserializator.h"


using namespace std;

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

int main(){
    const std::string& filepath = "/home/sobczakus/implementacja/git/ISBD2/batches/batches.bin";

    std::vector<Batch> batches = std::move(createSampleBatches());
    serializator(batches, filepath);
    std::vector<Batch> deserializated_batches = std::move(deserializator(filepath));
    calculateStatistics(deserializated_batches);
    validateBatches(batches, deserializated_batches);
}
