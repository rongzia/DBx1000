//
// Created by rrzhang on 2020/5/10.
//

#include <iostream>
#include <fstream>
using namespace std;

int main() {
    string path("/home/zhangrongrong/CLionProjects/DBx1000/server/workload/YCSB_schema.txt");

    string line;
    ifstream fin(path);
    getline(fin, line);
    cout << "size of first line :" << line.size() << endl;
    cout << "size of \"//size, type, name\" : " << string("//size, type, name").size() << endl;
    /// line do not contain '\n'



    return 0;
}