#pragma once

#include <string>
#include <libpq-fe.h>
#include "RsData.h"
#include "PgTable.h"
#include "PgSqlDatabase.h"
#include <vector>
#include <map>
#include "RsColumn.h"
#include <memory>
#include <unordered_map>

class PgSqlDatabase;

class PgTable {
public:
    PgTable(std::shared_ptr<PgSqlDatabase> db, const std::string &schema, const std::string &tableName);

    std::string printTableInfo();

    size_t getRowsCount();

    size_t getRowsCount(const std::string& toAdd);

    std::unordered_map<std::string, RsColumn> getColumns();

    std::vector<RsColumn> getColumnsOrdered();

    std::vector<RsValue> getPrimaryKeys();

    std::vector<RsValue> getForeignKeys();

    RsData findBy(const std::vector<std::string> &fields, const std::vector<std::string> &wheres,
                  const std::vector<std::string> &values);

    RsData getAllRows();

    RsData getAllRows(const std::string &toAdd);

    bool insert(const std::vector<std::string> &fields, const std::vector<std::string> &values);

    bool update(const std::vector<std::string> &fields, const std::vector<std::string> &values,
                const std::vector<std::string> &wheres, const std::vector<std::string> &whereValues);

    bool deleteWhere(const std::vector<std::string> &wheres = std::vector<std::string>(),
                     const std::vector<std::string> &whereValues = std::vector<std::string>());

private:
    std::shared_ptr<PgSqlDatabase> _db;
    std::string _schema;
    std::string _tableName;
    std::unordered_map<std::string, RsColumn> _columns;

    void fillColumnInfo();
};


