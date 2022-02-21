#include <iostream>

using namespace std;


void fun2(int& b) {
    b = 100;
}
void fun1(int* a) {
    cout << *a << ", " << __FILE__ << ", " << __LINE__ << endl;
    fun2(*a);
}


int main() {
    int* a = new int();
    *a = 10;
    fun1(a);
    cout << *a << ", " << __FILE__ << ", " << __LINE__ << endl;


    return 0;
} // g++ test2.cpp -o test2.exe