#pragma once


#include <sqlite3.h>
#include <string>
#include <map>
#include <vector>
#include "RsData.h"
#include "S3Table.h"

class S3Table;

class Sqlite3Database {
public:
    explicit Sqlite3Database(const std::string &database);

    bool execute(const std::string &sql);

    bool insert(const std::string &table_name, const std::map<std::string, std::string> &values);

    bool deleteRows(const std::string &table_name, const std::map<std::string, std::string> &values);

    bool update(const std::string &table_name, const std::string &key,
                const std::map<std::string, std::string> &values);

    bool clear(const std::string &table_name);

    RsData query(const std::string &qry);

    RsData query(const std::string &qry, const std::vector<std::string> &values);

    RsData query(const std::string &table_name, const std::vector<std::string> &columns,
                 const std::map<std::string, std::string> whereEquals);

    bool exists(const std::string &table_name, const std::string &column, const std::string& value);

    std::vector<unsigned char> getBlob(const std::string &sql);

    S3Table getTable(const std::string &tableName);

    std::string getLastError();

    ~Sqlite3Database();

    RsData getData(sqlite3_stmt *stmt, const std::string& qry);

private:
    sqlite3 *_db;

    bool checkError(int rc, char *errorMessage, const std::string &sql);

    bool checkError(int rc, char *errorMessage);

    bool checkError(int rc);

    bool _isOpen = false;

    static int readingCallback(void *data, int argc, char **argv, char **columnNames);

};


