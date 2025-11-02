#include "validator.h"
#include <iostream>
#include <fstream>

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
void validateBatches(std::vector<Batch>& batches, std::vector<Batch>& deserializated_batches) {
    uint32_t size = batches.size();
    for(uint32_t i = 0; i < size; i++) {
        checkEqualBatches(batches.at(i), deserializated_batches.at(i));
    }
}