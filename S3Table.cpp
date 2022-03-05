//
// Created by Bernhard Hermes on 06.07.18.
//

#include <FlowUtils/FlowLog.h>
#include "S3Table.h"

S3Table::S3Table(Sqlite3Database *db, const std::string &tableName) {
    this->_db = db;
    this->_tableName = tableName;
}

std::vector<RsValue> S3Table::getPrimaryKeys() {
    return std::vector<RsValue>();
}

std::vector<RsColumn> S3Table::getForeignKeys() {
    if (Columns.empty())
        fillColumnInfo();
    std::vector<RsColumn> rtn;
    for (auto &item: Columns)
        if (item.second.foreignKey == true)
            rtn.push_back(item.second);
    return rtn;
}

std::string S3Table::printTableInfo() {
    return std::string();
}

size_t S3Table::getRowsCount() {
    std::string sql = "SELECT Count(*) as rc FROM " + _tableName;
    auto data = _db->query(sql);
    if (data.empty()) return 0;
    return static_cast<size_t>(data["rc"].front().toLong());
}

std::map<std::string, RsColumn> S3Table::GetColumns() {
    if (Columns.empty()) {
        fillColumnInfo();
    }
    return Columns;
}

RsData S3Table::getFirstRow() {
    std::string sql = "SELECT * FROM " + _tableName + " LIMIT 1";
    return _db->query(sql);
}

bool S3Table::insert(const std::map<std::string, std::string> &values) {
    return _db->insert(_tableName, values);
}

void S3Table::fillColumnInfo() {
    std::string sql = "PRAGMA table_info(" + _tableName + ")";


    RsData data = _db->query(sql);
    size_t rows = data.rowSize();
    for (size_t i = 0; i < rows; ++i) {
        RsColumn column;
        column.name = data["name"].at(i).toString();
        column.type = data["type"].at(i).toString();
        column.null = !data["notnull"].at(i).toBoolean();
        column.primaryKey = data["pk"].at(i).toBoolean();
        Columns[column.name] = column;
    }

    std::string fksql = "pragma foreign_key_list(" + _tableName + ");";
    data = _db->query(fksql);
    rows = data.rowSize();
    for (size_t i = 0; i < rows; ++i) {
        std::string col = data["from"].at(i).toString();
        Columns[col].foreignKey = true;
        Columns[col].foreignKeyTable = data["table"].at(i).toString();
        Columns[col].foreignKeyColumn = data["to"].at(i).toString();;
    }

}

std::string S3Table::getGreatestOf(std::string column) {


    return std::string();
}
