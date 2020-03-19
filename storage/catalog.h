#pragma once 

#include <map>
#include <vector>
#include "global.h"
#include "helper.h"

//! 对列的描述，除了初始化，没有其他操作
class Column {
public:
	Column() {
		this->type = new char[80];
		this->name = new char[80];
	}
	Column(uint64_t size, char * type, char * name, 
		uint64_t id, uint64_t index) 
	{
		this->size = size;
		this->id = id;
		this->index = index;
		this->type = new char[80];
		this->name = new char[80];
		strcpy(this->type, type);
		strcpy(this->name, name);
	};

	UInt64 id;      //! 处在第几列
	UInt32 size;    //! 该列的大小 ( in bytes )
	UInt32 index;   //! 处在该行的第多少字节 ( in bytes )
	char * type;    //! 类型
	char * name;    //! 列名
	char pad[CL_SIZE - sizeof(uint64_t)*3 - sizeof(char *)*2];
};

//! 相当于表空间，列的集合，函数都是对列的操作，不涉及行的操作
class Catalog {
public:
	// abandoned init function
	// field_size is the size of each each field.
	void init(const char * table_name, int field_cnt);
	void add_col(char * col_name, uint64_t size, char * type);

	UInt32 			field_cnt;     //! 列数
 	const char * 	table_name;
	
	UInt32 			get_tuple_size() { return tuple_size; };
	
	uint64_t 		get_field_cnt() { return field_cnt; };
	uint64_t 		get_field_size(int id) { return _columns[id].size; };
	uint64_t 		get_field_index(int id) { return _columns[id].index; };
	char * 			get_field_type(uint64_t id);
	char * 			get_field_name(uint64_t id);
	uint64_t 		get_field_id(const char * name);
	char * 			get_field_type(char * name);
	uint64_t 		get_field_index(char * name);

	void 			print_schema();
	Column * 		_columns;       //! 列
	UInt32 			tuple_size;     //! 一行的大小 (in bytes)
};

