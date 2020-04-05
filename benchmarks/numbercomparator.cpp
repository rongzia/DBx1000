//
// Created by rrzhang on 2020/4/5.
//
#include "numbercomparator.h"
#include "no_destructor.h"
#include "leveldb/slice.h"

namespace leveldb {
    NumberComparatorImpl::NumberComparatorImpl() {}

    const char *NumberComparatorImpl::Name() const {
        return "leveldb.NumberComparator";
    }

    int NumberComparatorImpl::Compare(const leveldb::Slice &a, const leveldb::Slice &b) const {
        if (a.size() == b.size()) {
            return a.compare(b);
        } else {
            return a.size() > b.size() ? +1 : -1;
        }
    }

    void NumberComparatorImpl::FindShortestSeparator(std::string *start, const leveldb::Slice &limit) const {}

    void NumberComparatorImpl::FindShortSuccessor(std::string *key) const {}

    const leveldb::Comparator *NumberComparator() {
        static leveldb::NoDestructor<NumberComparatorImpl> singleton;
        return singleton.get();
    }

}
