#pragma once
#include <string>
#include <iostream>
#include <vector>
#include <cstring>

//results
using std::string;
using int_t = uint32_t;

enum MetaCommandResult
{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
};

enum StatementType
{
	STATEMENT_INSERT,
	STATEMENT_SELECT
};

enum PrepareResult
{
	PREPARE_SUCCESS,
	PREPARE_UNRECOGNIZED_STATEMENT,
	PREPARE_SYNTAX_ERROR
};

enum ExecuteResult
{
	EXECUTE_FAILURE_FULL,
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL,
	EXECUTE_DUPLICATE
};


enum NodeType
{
	NODE_INTERNAL,
	NODE_LEAF
};

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255


/// @brief class Row is the basic storage structure which supports the storage of [id, username, email].
class Row
{
public:
	int_t id;
	char username[COLUMN_USERNAME_SIZE];
	char email[COLUMN_EMAIL_SIZE];
};


#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

const int_t ID_SIZE = size_of_attribute(Row, id);
const int_t USERNAME_SIZE = size_of_attribute(Row, username);
const int_t EMAIL_SIZE = size_of_attribute(Row, email);
const int_t ID_OFFSET = 0;
const int_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const int_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const int_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

/// @brief serialize the row object into a string
/// @param Row source 
/// @param void* destination 
void serialize_row(Row& source, void* destination)
{
	memcpy((char*)destination + ID_OFFSET, &(source.id), ID_SIZE);
	memcpy((char*)destination + USERNAME_OFFSET, &(source.username), USERNAME_SIZE);
	memcpy((char*)destination + EMAIL_OFFSET, &(source.email), EMAIL_SIZE);
}

/// @brief desrialize the string version of row into row object
/// @param void* source 
/// @param Row& destination 
void deserialize_row(void* source, Row& destination)
{
	memcpy(&(destination.id), (char*)source + ID_OFFSET, ID_SIZE);
	memcpy(&(destination.username), (char*)source + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(destination.email), (char*)source + EMAIL_OFFSET, EMAIL_SIZE);
}

/// @brief class Table and related stuffs
#define TABLE_MAX_PAGES 100
const int_t PAGE_SIZE = 4096;
const int_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const int_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

/*B+ Tree implementation

1.only leaf node contains key:value pairs
  header only contains id, is_root, parent_pointer.
2.it is a BST, searching is based on the id.

structure:
		[header][header]
			/		   \
[header][header]	[header][header]
		/ 					/  \
[leaf node] 				[leafnode] [leafnode]

*/

/*
 * Common Node Header Layout:
 * structure be like:
 * 
 * total size 12
 * [int node_type|int is_root| void* parent_pointer]
 * [      4      |      4    |      	4		   ]
 */
//size of node_type, it is an integer with size == 4
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
//size of is_root, it is an integer with size == 4
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
//size of parent_pointer, it is an pointer with size == 4
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;


/*
 * Leaf Node Header Layout
 * structure be like:
 * 
 * total size 16
 * [int node_type|int is_root| void* parent_pointer|int num_cells]
 * [      4      |     4     |      	4		   |	  4   	 ]
 * 
 */
//cell size
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
//cell offset
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
//header size
const uint32_t LEAF_NODE_HEADER_SIZE = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;

/*
 *  Leaf Node structure be like:
 *  
 *	total size: 16 + num_cell * (4+row_size)
 * 	[HEADER|     CELL    |     CELL    |     CELL    |...|...]
 *  [  16  |  4+row_size |  4+row_size |  4+row_size |...|...]
 *   
 *  cell structure be like:
 *
 *	[KEY|  VALUE ]
 *  [4  |row_size]
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;


class LeafNode
{
private:
	void* node;

public:
	LeafNode(void* node) : node(node) {}
	void initialize_leaf_node()
	{
		*leaf_node_num_cells() = 0;
	}
	uint32_t* leaf_node_num_cells()
	{
		return (uint32_t*)((char*)node + LEAF_NODE_NUM_CELLS_OFFSET);
	}
	void* leaf_node_cell(uint32_t cell_num)
	{
		return (char*)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
	}
	uint32_t* leaf_node_key(uint32_t cell_num)
	{
		return (uint32_t*)leaf_node_cell(cell_num);
	}
	void* leaf_node_value(uint32_t cell_num)
	{
		return (char*)leaf_node_cell(cell_num) + LEAF_NODE_KEY_SIZE;
	}
	void print_leaf_node()
	{
		uint32_t num_cells = *leaf_node_num_cells();
		std::cout << "leaf (size " << num_cells << ")" << std::endl;
		for (uint32_t i = 0; i < num_cells; i++)
		{
			uint32_t key = *leaf_node_key(i);
			std::cout << "  - " << i << " : " << key << std::endl;
		}
	}
	NodeType get_node_type()
	{
		uint8_t value = *((uint8_t *)((char *)node + NODE_TYPE_OFFSET));
		return (NodeType)value;
	}	
	void set_node_type(NodeType type)
	{
		*((uint8_t *)((char*)node + NODE_TYPE_OFFSET)) = (uint8_t)type;
	}
};


class Pager
{
public:
	int fd;
	int_t file_length;
	int_t num_page;
	void* pages[TABLE_MAX_PAGES];
	Pager(char* filename);
	void* get_page(int_t page_num);
	void pager_flush(int_t page_num);
};


/// @brief class Table is used to store pages, each pages contains ROWS_PER_PAGE rows.
class Cursor;
class Table
{
public:
	uint32_t root_page_num;
	Pager pager;

public:
	Table(char* filename) : pager(filename)
	{
		root_page_num = 0;
		if (pager.num_page == 0)
		{
			LeafNode root_node = pager.get_page(0);
			root_node.initialize_leaf_node();
		}
	}
	// using binary search to find the insert position.
	Cursor* table_find(uint32_t key);
	~Table();
};



class Statement
{
public:
	Statement() = default;
	StatementType type;
	Row row_to_insert;
};

class Cursor
{
public:
	Table* table;
	int_t page_num;
	int_t cell_num;
	bool end_of_table;

public:
	Cursor(Table*& table, bool option);
	Cursor(Table* table, uint32_t page_num, uint32_t key);
	void leaf_node_insert(uint32_t key, Row& value);
	void* cursor_value();
	void cursor_advance();
};



class DB
{
public:
	DB(char* filename);
	~DB();
	void start();
	void print_prompt();
	PrepareResult prepare_statement(string& input_line, Statement& statement);
	bool parse_statement(string& input_line, Statement& statement);
	void execute_statement(Statement& statement);
	bool parse_meta_command(string& command);
	MetaCommandResult do_meta_command(string& command);
	ExecuteResult execute_insert(Statement& statement);
	ExecuteResult execute_select(Statement& statement);

private:
	Table* table;
};
