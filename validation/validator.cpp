#include "validator.h"
#include <iostream>
#include <fstream>

void checkIntColumn(IntColumn& first, IntColumn& second){
    if (first.name != second.name) {
        std::cerr<<"Expected name: " << first.name << " but received: " <<  second.name << "\n";  
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
        std::cerr<<"Expected name: " << first.name << " but received: " <<  second.name << "\n";  
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
void printIntColumn(const IntColumn& col) {
    std::cout << "IntColumn: " << col.name << " (rows: " << col.column.size() << ")\n";
    std::cout << "Values: ";
    for (size_t i = 0; i < col.column.size(); ++i) {
        if (i) std::cout << ", ";
        std::cout << col.column[i];
    }
    std::cout << '\n';
}

void printStringColumn(const StringColumn& col) {
    std::cout << "StringColumn: " << col.name << " (rows: " << col.column.size() << ")\n";
    std::cout << "Values: ";
    for (size_t i = 0; i < col.column.size(); ++i) {
        if (i) std::cout << ", ";
        std::cout << '"' << col.column[i] << '"';
    }
    std::cout << '\n';
}

void printBatch(const Batch& b) {
    std::cout << "Batch: num_rows=" << b.num_rows
              << ", intColumns=" << b.intColumns.size()
              << ", stringColumns=" << b.stringColumns.size() << '\n';
    for (const auto& ic : b.intColumns) {
        printIntColumn(ic);
    }
    for (const auto& sc : b.stringColumns) {
        printStringColumn(sc);
    }
}

void validateBatches(std::vector<Batch>& batches, std::vector<Batch>& deserializated_batches) {
    uint32_t size = batches.size();
    for(uint32_t i = 0; i < size; i++) {
        // std::cout<< "Batch number " << i << "\n";
        // printBatch(batches.at(i));
        checkEqualBatches(batches.at(i), deserializated_batches.at(i));
    }
}