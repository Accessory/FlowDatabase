#pragma once


#include "Sqlite3Database.h"
#include "RsColumn.h"
#include <string>
#include <vector>
#include <map>

class Sqlite3Database;

class S3Table {
public:
    S3Table(Sqlite3Database *db, const std::string &tableName);

    std::string printTableInfo();

    size_t getRowsCount();

    std::vector<RsValue> getPrimaryKeys();

    std::vector<RsColumn> getForeignKeys();

    std::map<std::string, RsColumn> GetColumns();

    std::map<std::string, RsColumn> Columns;

    RsData getFirstRow();

    bool insert(const std::map<std::string, std::string> &values);

    std::string getGreatestOf(std::string column);

private:
    Sqlite3Database *_db;
    std::string _tableName;

    void fillColumnInfo();
};


