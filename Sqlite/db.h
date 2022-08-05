#pragma once
#include <string>
#include <iostream>
#include <vector>

//update
using std::string;

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
	PREPARE_UNRECOGNIZED_STATEMENT
};

class Statement
{
public:
	Statement() = default;
	StatementType type;
};

class DB
{
public:
	void start();
	void print_prompt();
	PrepareResult prepare_statement(string& input_line, Statement& statement);
	bool parse_statement(string& input_line, Statement& statement);
	void execute_statement(Statement& statement);
	bool parse_meta_command(string& command);
	MetaCommandResult do_meta_command(string& command);
};