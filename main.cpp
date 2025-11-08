#include <iostream>
#include <fstream>
#include <unordered_map>

using namespace std;

extern void simpleTest();
extern void columnTest();
extern void someFilesTest();
extern void bigTest();
extern void clearAfterTests();

int main(){
    columnTest();
    simpleTest();
    someFilesTest();
    bigTest();
    clearAfterTests();
    return 0;
}
