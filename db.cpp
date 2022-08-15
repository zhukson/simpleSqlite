#define _CRT_SECURE_NO_WARNINGS
#include "db.h"
#include <unistd.h>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

using namespace std;

DB::DB(char* filename)
{
	table = new Table(filename);
}

DB::~DB()
{
	delete table;
}

//update
/// @brief implementation of REPL
void DB::start()
{
	while (true) {
		print_prompt();
		string input_line;
		getline(cin, input_line);
		if (parse_meta_command(input_line))
		{
			continue;
		}
		Statement statement;
		if (!parse_statement(input_line, statement))
		{
			continue;
		}
		execute_statement(statement);
	}
}

/// @brief print the prompt
void DB::print_prompt()
{
	cout << "db > " << endl;
}


/// @brief parse the meta command
/// @param string command 
/// @return true if it is a meta command, otherwise false
bool DB::parse_meta_command(string& command)
{
	if (command[0] == '.') {
		switch (do_meta_command(command))
		{
		case META_COMMAND_SUCCESS:
			return true;
			break;
		case META_COMMAND_UNRECOGNIZED_COMMAND:
			cout << "unrecognized_command: " << command << endl;
			return true;
			break;
		}
	}
	return false;
}

/// @brief execute the meta command
/// @param string command 
/// @return  
MetaCommandResult DB::do_meta_command(string& command)
{
	if (command == ".exit" || command == ".quit")
	{
		delete(table);
		cout << "see you next time!" << endl;
		exit(0);
	}
	else if (command == ".clear")
	{
		system("cls");
		return META_COMMAND_SUCCESS;
	}
	else if (command == ".btree")
	{
		std::cout << "Tree:" << std::endl;
		LeafNode root_node = table->pager.get_page(table->root_page_num);
		root_node.print_leaf_node();
		return META_COMMAND_SUCCESS;
	}
	else if (command == ".constants")
	{
		std::cout << "Constants:" << std::endl;
		std::cout << "ROW_SIZE: " << ROW_SIZE << std::endl;
		std::cout << "COMMON_NODE_HEADER_SIZE: " << COMMON_NODE_HEADER_SIZE << std::endl;
		std::cout << "LEAF_NODE_HEADER_SIZE: " << LEAF_NODE_HEADER_SIZE << std::endl;
		std::cout << "LEAF_NODE_CELL_SIZE: " << LEAF_NODE_CELL_SIZE << std::endl;
		std::cout << "LEAF_NODE_SPACE_FOR_CELLS: " << LEAF_NODE_SPACE_FOR_CELLS << std::endl;
		std::cout << "LEAF_NODE_MAX_CELLS: " << LEAF_NODE_MAX_CELLS << std::endl;
		return META_COMMAND_SUCCESS;
	}
	else
	{
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

/// @brief check if the statement is valid or not.
/// @param string& input_line 
/// @param Satement& statement 
/// @return 
PrepareResult DB::prepare_statement(string& input_line, Statement& statement)
{
	if (input_line.compare(0, 6, "insert") == 0)
	{
		statement.type = STATEMENT_INSERT;
		int args_assigned = std::sscanf(
			input_line.c_str(), "insert %d %s %s", &(statement.row_to_insert.id),
			statement.row_to_insert.username, statement.row_to_insert.email);
		if (args_assigned != 3)
			return PREPARE_SYNTAX_ERROR;
		return PREPARE_SUCCESS;
	}
	else if (input_line.compare(0, 6, "select") == 0 || input_line.compare(0, 6, "SELECT") == 0)
	{
		statement.type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	else
	{
		return PREPARE_UNRECOGNIZED_STATEMENT;
	}
}

/// @brief check if the statement is valid or not.
/// @param string& input_line 
/// @param Statement& statement 
/// @return 
bool DB::parse_statement(string& input_line, Statement& statement)
{
	if (input_line[0] != '.')
	{
		switch (prepare_statement(input_line, statement)) {
		case PREPARE_SUCCESS:
			return true;
			break;
		case PREPARE_UNRECOGNIZED_STATEMENT:
			cout << "unrecognized statement:" << input_line << endl;
			return false;
			break;
		case PREPARE_SYNTAX_ERROR:
			cout << "syntax error!" << input_line << endl;
			return false;
			break;
		}
	}
	return false;
}

/// @brief executing the statment
/// @param statement 
void DB::execute_statement(Statement& statement)
{
	ExecuteResult res;
	switch (statement.type)
	{
	case STATEMENT_INSERT:
		//cout << "executing insert statement" << endl;
		res = execute_insert(statement);
		break;
	case STATEMENT_SELECT:
		//cout << "executing select statement" << endl;
		res = execute_select(statement);
		break;
	}
	switch (res)
	{
	case EXECUTE_FAILURE_FULL:
		cout << "ERROR: TABLE IS FULL!" << endl;
		break;
	case EXECUTE_SUCCESS:
		cout << "done." << endl;
		break;
	case EXECUTE_DUPLICATE:
		cout << "duplicated key!" << endl;
		break;
	}
}

/// @brief execution function for insert command
/// @param Statement& statement 
/// @param Table& table 
/// @return execution result
ExecuteResult DB::execute_insert(Statement& statement)
{
	LeafNode leaf_node = table->pager.get_page(table->root_page_num);
	uint32_t num_cells = *leaf_node.leaf_node_num_cells();
	if (*(leaf_node.leaf_node_num_cells()) >= LEAF_NODE_MAX_CELLS)
	{
		std::cout << "Leaf node full." << std::endl;
		return EXECUTE_TABLE_FULL;
	}

	// end of the table
	Cursor* cursor = table->table_find(statement.row_to_insert.id);
	
	//check dupicate key
	if(cursor->cell_num < num_cells)
	{
		uint32_t key_at_index = *leaf_node.leaf_node_key(cursor->cell_num);
		if(key_at_index == statement.row_to_insert.id)
		{
			return EXECUTE_DUPLICATE;
		}
	}
	cursor->leaf_node_insert(statement.row_to_insert.id, statement.row_to_insert);

	delete cursor;

	return EXECUTE_SUCCESS;
}

ExecuteResult DB::execute_select(Statement& statement)
{
	Cursor* cursor = new Cursor(table, true);
	Row row;
	while(!cursor->end_of_table)
	{	
		deserialize_row(cursor->cursor_value(), row);
		cout << "(" << row.id << "," << row.username << "," << row.email << ")" << endl;
		cursor->cursor_advance();
	}
	delete(cursor);
	return EXECUTE_SUCCESS;
}

Pager::Pager(char* filename)
{
	fd = open(filename, O_RDWR | O_CREAT);
	if (fd < 0)
	{
		cerr << "ERROR: cannot open file" << filename << endl;
		exit(1);
	}
	file_length = lseek(fd, 0, SEEK_END);
	num_page = file_length / PAGE_SIZE;
	if (file_length % PAGE_SIZE != 0)
	{
		cerr << "num_page is not an integer." << endl;
		exit(1);
	}
	for (int_t i = 0; i < TABLE_MAX_PAGES; ++i)
	{
		pages[i] = nullptr;
	}
}

/// @brief get the memory address of the page given the page_num, if it is NULL, allocate new memory.
/// @param page_num 
/// @return memory address of the page.
void* Pager::get_page(int_t page_num)
{
	if (page_num > TABLE_MAX_PAGES)
	{
		cout << "ERROR: page_num out of bound" << endl;
		exit(1);
	}
	if (pages[page_num] == nullptr)
	{
		void* page = malloc(PAGE_SIZE);
		int_t num_pages = file_length % PAGE_SIZE ? (file_length / PAGE_SIZE) + 1 : file_length / PAGE_SIZE;
		if (page_num <= num_pages)
		{
			lseek(fd, page_num * PAGE_SIZE, SEEK_SET);
			ssize_t bytes_read = read(fd, page, PAGE_SIZE);
			if (bytes_read == -1) {
				perror("read file error");
				exit(1);
			}
		}
		pages[page_num] = page;

		if (page_num >= num_pages) {
			this->num_page = page_num + 1;
		}
	}
	return pages[page_num];
}


void Pager::pager_flush(int_t page_num)
{
	if (pages[page_num] == nullptr)
	{
		std::cout << "Tried to flush null page" << std::endl;
		exit(EXIT_FAILURE);
	}

	off_t offset = lseek(fd, page_num * PAGE_SIZE, SEEK_SET);

	if (offset == -1)
	{
		std::cout << "Error seeking: " << errno << std::endl;
		exit(EXIT_FAILURE);
	}

	ssize_t bytes_written =
		write(fd, pages[page_num], PAGE_SIZE);

	if (bytes_written == -1)
	{
		std::cout << "Error writing: " << errno << std::endl;
		exit(EXIT_FAILURE);
	}
}

Table::~Table()
{	
	for (int_t i = 0; i < pager.num_page; ++i)
	{
		if (pager.pages[i] == nullptr)
		{
			continue;
		}
		pager.pager_flush(i);
		free(pager.pages[i]);
		pager.pages[i] = nullptr;
	}
	int result = close(pager.fd);
	if (result == -1) 
	{
		cout << "ERROR: closing db file" << endl;
		exit(1);
	}
}

Cursor *Table::table_find(uint32_t key)
{
	LeafNode root_node = pager.get_page(root_page_num);
	return new Cursor(this, root_page_num, key);
}

Cursor::Cursor(Table *table, uint32_t page_num, uint32_t key)
{
	this->table = table;
	this->page_num = page_num;
	this->end_of_table = false;

	LeafNode root_node = table->pager.get_page(page_num);
	uint32_t num_cells = *root_node.leaf_node_num_cells();

	//binary search
	uint32_t l = 0, r = num_cells-1;
	while(l <= r)
	{
		uint32_t m = l + ((r-l)>>1);
		uint32_t cur_key = *root_node.leaf_node_key(m);
		if(cur_key > key)
		{
			r = m-1;
		}
		else if (cur_key < key)
		{
			l = m+1;
		}
		else
		{
			this->cell_num = m;
			return;
		}
	}

	this->cell_num = l;
}



/// @brief obtain the address of the target row
/// @param table 
/// @param row_num 
/// @return the address



int main(int argc, char* argv[])
{
	if (argc < 2)
	{
		cout << "database file is missing!" << endl;
		exit(1);
	}
	DB db(argv[1]);
	db.start();
	return 0;
}

Cursor::Cursor(Table*& table, bool option)
{
	this->table = table;
	page_num = table->root_page_num;
	LeafNode root_node = table->pager.get_page(table->root_page_num);
	int_t num_cells = *root_node.leaf_node_num_cells();

	if (option)
	{
		cell_num = 0;
		end_of_table = (num_cells == 0);
	}
	else
	{
		cell_num = num_cells;
		end_of_table = true;
	}
}

void Cursor::leaf_node_insert(uint32_t key, Row& value)
{
	LeafNode leaf_node = table->pager.get_page(page_num);
	uint32_t num_cells = *leaf_node.leaf_node_num_cells();

	if (num_cells >= LEAF_NODE_MAX_CELLS)
	{
		// Node full
		std::cout << "Need to implement splitting a leaf node." << std::endl;
		exit(EXIT_FAILURE);
	}

	if (cell_num < num_cells)
	{
		// make room for new cell
		for (uint32_t i = num_cells; i > cell_num; i--)
		{
			memcpy(leaf_node.leaf_node_cell(i), leaf_node.leaf_node_cell(i - 1),
				LEAF_NODE_CELL_SIZE);
		}
	}

	// insert new cell
	*(leaf_node.leaf_node_num_cells()) += 1;
	*(leaf_node.leaf_node_key(cell_num)) = key;
	serialize_row(value, leaf_node.leaf_node_value(cell_num));
}



void* Cursor::cursor_value()
{
	void* page = table->pager.get_page(page_num);
	return LeafNode(page).leaf_node_value(cell_num);
}

void Cursor::cursor_advance()
{
	++cell_num;
	LeafNode root_node = table->pager.get_page(table->root_page_num);
	if (cell_num >= *root_node.leaf_node_num_cells())
	{
		end_of_table = true;
	}
}
