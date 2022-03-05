#include <FlowUtils/FlowFile.h>
#include <FlowUtils/FlowString.h>
#include "FlowPgGod.h"
#include "PgSqlDatabase.h"
#include <FlowUtils/FlowLog.h>
#include "FlowDatabaseGod.h"

std::vector<std::string> FlowPgGod::getTableNames(const std::string &connectionInfo, const std::string &schema) {
    PgSqlDatabase db(connectionInfo);
    RsData data = db.query("SELECT table_name FROM information_schema.tables WHERE table_schema='" + schema + "';");
    return data.toStringVector("table_name");
}

std::vector<std::string>
FlowPgGod::getTableColumns(const std::string &connectionInfo, const std::string &schema, const std::string &table) {
    PgSqlDatabase db(connectionInfo);
    std::vector<std::string> params;
    params.emplace_back(schema);
    params.emplace_back(table);
    RsData data = db.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2", params);
    return data.toStringVector("column_name");
}

void FlowPgGod::printSql(const std::string &connectionInfo, const std::string &sql) {
    PgSqlDatabase db(connectionInfo);
    RsData data = db.query(sql);
    LOG_INFO << "Number Rows: " << data.rowSize();
    LOG_INFO << std::endl << data.printData();
}

void
FlowPgGod::printTableInfo(const std::string &connectionInfo, const std::string &schema, const std::string &tableName) {
    PgSqlDatabase db(connectionInfo);

    PgTable table = db.getTable(schema, tableName);

    LOG_INFO << "Number Rows: " << table.getRowsCount();
    auto columns = table.getColumns();
    std::stringstream columnP;
    size_t i = 0;
    for (auto column : columns) {
        if (i++ > 0)
            columnP << ", ";
        columnP << column.first;
    }
    LOG_INFO << "Columns: " << columnP.str();
    auto pkeys = table.getPrimaryKeys();

    std::stringstream pkeysP;
    for (i = 0; i < pkeys.size(); ++i) {
        if (i > 0)
            pkeysP << ", ";
        pkeysP << pkeys.at(i).toString();
    }
    LOG_INFO << "Primary Keys: " << pkeysP.str();

    auto fkeys = table.getForeignKeys();
    std::stringstream fkeysP;
    for (i = 0; i < fkeys.size(); ++i) {
        if (i > 0)
            fkeysP << ", ";
        fkeysP << fkeys.at(i).toString();
    }
    LOG_INFO << "Foreign Keys: " << fkeysP.str();

}

void FlowPgGod::newPgDB(const std::string &connectionInfo, const std::string &name) {
    PgSqlDatabase db(connectionInfo);

    db.execute("UPDATE pg_database SET datallowconn = 'false' WHERE datname = '" + name + "';");
    db.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + name + "';");
    db.execute("DROP DATABASE IF EXISTS \"" + name + "\"");
    db.execute("CREATE DATABASE \"" + name + "\"");
}

void FlowPgGod::dbSchemaToJson(const std::string &connectionInfo, const std::string &schema, const std::string &path) {
    PgSqlDatabase db(connectionInfo);
    RsData data = db.query("SELECT table_name FROM information_schema.tables WHERE table_schema='" + schema + "';");
    std::vector<std::string> tableNames = data.toStringVector("table_name");
    FlowFile::createDirIfNotExist(path, false);
    for (std::string tableName : tableNames) {
        PgTable table = db.getTable(schema, tableName);
        auto info = table.getColumnsOrdered();
        auto jsonString = FlowDatabaseGod::generateJson(tableName, schema, info);
        std::string fileName = FlowFile::combinePath(path, tableName + ".json");
        FlowFile::stringToFile(fileName, jsonString);
    }
}


