//
// Created by rrzhang on 2020/4/27.
//

#include <iostream>
#include <cstring>
using namespace std;


class A{
public:
    A(char *p) {
        p_ = p;
    }
    ~A() {
        cout << "~A" << endl;
//        delete p_;
    }
    char *p_;
};



class B {
public:
    explicit B(A* a) {
        a_ = a;
    }
    ~B() {
        cout << "~B" << endl;
        delete a_;
    }
    A* a_;
};

void test(char *p) {
    A *a = new A(p);
    cout << "A::p_ : " << a->p_ << endl;

    B *b = new B(a);

    delete b;

    A *a2 = new A(p);
    B *b2 = new B(a2);
    b = b2;
    cout << "test b->a->p_ : " << b->a_->p_ << endl;
//    delete(a);
    cout << "test p : " << p << endl;
}


int main() {
    char *p = "adf";
    cout << "main p : " << p << endl;
    test(p);
    cout << "main p : " << p << endl;

    char *p4 = new char[3];
    cout<< (nullptr == p4 ? "null" : "not null") << endl;

    delete p4;
    p4 = NULL;
    cout << (NULL == nullptr ? "yes" : "no") << endl;
    cout<< (nullptr == p4 ? "null" : "not null") << endl;
    p4 = "";
    cout<< (nullptr == p4 ? "null" : "not null") << endl;

    char* p2 = nullptr;
    char* p3 = nullptr;
    memcpy(p2, p3, 0);


    return 0;
}