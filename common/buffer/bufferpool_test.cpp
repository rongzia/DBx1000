#include <iostream>
#include "bufferpool.h"
using namespace std;


int main() {
    // BufferPool *bufferpool = new BufferPool();
    BufferPool bufferpool;
    bufferpool.SetSize(2);

    Page page1;
    page1.Init();
    memset(page1.page_buf(), '5', 74);
    page1.set_page_id(1);
    page1.Print();

    const BufferPool::PageHandle* handle1 = 
            bufferpool.Put(std::make_pair<TABLES, uint64_t>(TABLES::MAIN_TABLE, page1.page_id()), page1);
    cout << endl;

    dbx1000::Page page2(page1);
    page2.set_page_id(2);
    page2.Print();
    const BufferPool::PageHandle* handle2 = 
            bufferpool.Put(std::make_pair<TABLES, uint64_t>(TABLES::MAIN_TABLE, page1.page_id()), std::move(page2));
    cout << endl;

    const BufferPool::PageHandle* handle3 = 
            bufferpool.Put(std::make_pair<TABLES, uint64_t>(TABLES::MAIN_TABLE, page1.page_id()), Page(new char[MY_PAGE_SIZE]));

    Page page4;
    page4.Init();
    const BufferPool::PageHandle* handle4 = 
            bufferpool.Put(std::make_pair<TABLES, uint64_t>(TABLES::MAIN_TABLE, page1.page_id()), Page(page4));

    Page* page5 = new Page();
    page5->Init();
    const BufferPool::PageHandle* handle5 = 
            bufferpool.Put(std::make_pair<TABLES, uint64_t>(TABLES::MAIN_TABLE, page1.page_id()), Page(*page5));

    bufferpool.Release(handle1);
    bufferpool.Release(handle2);

    page1.Print();
    page2.Print();
    handle3->value.Print();
    bufferpool.Release(handle3);

    bufferpool.Release(handle4);
    bufferpool.Release(handle5);

    // char* buf1;
    // delete [] buf1;
    

    return 0;
}
// g++ common/storage/tablespace/page.cpp common/buffer/bufferpool.h common/buffer/bufferpool_test.cpp -o test.exe -lpthread