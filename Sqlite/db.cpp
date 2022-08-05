#include "DB.h"

using namespace std;

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
		cout << "see you next time!" << endl;
		exit(0);
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
	if (input_line.compare(0, 6, "insert") == 0 || input_line.compare(0, 6, "INSERT") == 0)
	{
		statement.type = STATEMENT_INSERT;
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
			return true;
			break;
		}
	}
	return false;
}

/// @brief executing the statment
/// @param statement 
void DB::execute_statement(Statement& statement)
{
	switch (statement.type) {
	case STATEMENT_INSERT:
		cout << "executing insert statement" << endl;
		break;
	case STATEMENT_SELECT:
		cout << "executing select statement" << endl;
		break;
	} 
}

int main()
{
	DB db;
	db.start();
	return 0;
}
