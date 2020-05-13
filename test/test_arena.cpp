//
// Created by rrzhang on 2020/5/7.
//

#include <iostream>
#include <util/arena.h>
using namespace std;


class A{
public:
    A(int a) : a_(a){

    }
    ~A() {
        cout << "~A" << endl;
    }
    int a_;
};

int main(){
    dbx1000::Arena* arena = new dbx1000::Arena();
    A* a = new (arena->Allocate(sizeof(A*)))A(3);
    cout << "(a->a_ : )" << a->a_ << endl;
    cout << "size of A* :" << sizeof(A*) << endl;
    cout << "size of A :" << sizeof(A) << endl;
    cout << "size of arena :" << arena->MemoryUsage() << endl;

    delete(arena);
    cout << "(a->a_ : )" << a->a_ << endl;




    return 0;
}