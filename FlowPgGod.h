#pragma once

#include <vector>
#include <string>
#include "RsColumn.h"


class FlowPgGod {
public:
    static std::vector<std::string> getTableNames(const std::string &connectionInfo, const std::string &schema);

    static std::vector<std::string> getTableColumns(const std::string &connectionInfo, const std::string &schema, const std::string &table);

    static void printTableInfo(const std::string &connectionInfo, const std::string &schema, const std::string &table);

    static void printSql(const std::string &connectionInfo, const std::string &sql);

    static void newPgDB(const std::string &connectionInfo, const std::string &name);

    static void dbSchemaToJson(const std::string &connectionInfo, const std::string &schema, const std::string &path);

};


