#include "PgTable.h"
#include <sstream>
#include <algorithm>
#include "DbUtil.h"

PgTable::PgTable(std::shared_ptr<PgSqlDatabase> db, const std::string &schema, const std::string &tableName)
: _db(db){
    this->_schema = schema;
    this->_tableName = tableName;
}

std::string PgTable::printTableInfo() {
    const auto columns = getColumns();
    std::stringstream rtn;
    rtn << "Table: " << this->_tableName << std::endl;
    rtn << "Columns:";
    for (const auto column : columns) {
        rtn << " " << column.first;
    }
    rtn << std::endl;
    rtn << "Row count: " << getRowsCount() << std::endl;
    return rtn.str();
}

size_t PgTable::getRowsCount() {
    std::string sql = "SELECT count(*) as rc FROM " + _schema + "." + _tableName;
    RsData data = _db->query(sql);
    if (data.empty()) return 0;
    return static_cast<size_t>(data["rc"].front().toLong());
}

size_t PgTable::getRowsCount(const std::string& toAdd) {
    std::string sql = "SELECT count(*) as rc FROM " + _schema + "." + _tableName + " " + toAdd;
    RsData data = _db->query(sql);
    if (data.empty()) return 0;
    return static_cast<size_t>(data["rc"].front().toLong());
}

std::unordered_map<std::string, RsColumn> PgTable::getColumns() {
    if (_columns.empty()) {
        fillColumnInfo();
    }
    return _columns;
}

void PgTable::fillColumnInfo() {
    std::vector<std::string> params;
    params.emplace_back(_schema);
    params.emplace_back(_tableName);
    RsData data = _db->query(
            "SELECT column_name, udt_name, is_nullable, ordinal_position FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 order by ordinal_position",
            params);

    size_t rows = data.rowSize();
    for (size_t i = 0; i < rows; ++i) {
        RsColumn column;
        column.name = data["column_name"].at(i).toString();
        column.type = data["udt_name"].at(i).toString();
        column.null = data["is_nullable"].at(i).toString() == "YES";
        column.ordinalPosition = static_cast<size_t>(data["ordinal_position"].at(i).toInteger());
        _columns[column.name] = column;
    }

    params.emplace_back("PRIMARY KEY");
    data = _db->query(
            "SELECT t2.column_name FROM information_schema.table_constraints as t1 JOIN "
            "information_schema.constraint_column_usage as t2 ON t1.constraint_name = t2.constraint_name WHERE "
            "t1.table_schema = $1 AND t1.table_name = $2 AND t1.constraint_type = $3",
            params);
    rows = data.rowSize();
    for (size_t i = 0; i < rows; ++i) {
        std::string id = data["column_name"].at(i).toString();
        _columns[id].primaryKey = true;
    }

    params.pop_back();
    params.emplace_back("UNIQUE");
    data = _db->query(
            "SELECT t2.column_name FROM information_schema.table_constraints as t1 JOIN "
            "information_schema.constraint_column_usage as t2 ON t1.constraint_name = t2.constraint_name WHERE "
            "t1.table_schema = $1 AND t1.table_name = $2 AND t1.constraint_type = $3",
            params);
    rows = data.rowSize();
    for (size_t i = 0; i < rows; ++i) {
        std::string id = data["column_name"].at(i).toString();
        _columns[id].unique = true;
    }

    params.pop_back();
    params.emplace_back("FOREIGN KEY");
    data = _db->query(
            "SELECT t2.column_name as cn, t3.table_name as tn, t3.column_name as fcn FROM information_schema.table_constraints as t1 "
            "JOIN information_schema.key_column_usage t2 ON t1.constraint_name = t2.constraint_name "
            "JOIN information_schema.constraint_column_usage as t3 ON t1.constraint_name = t3.constraint_name "
            "WHERE t1.table_schema = $1 AND"
            " t1.table_name = $2 AND"
            " t1.constraint_type = $3",
            params);
    rows = data.rowSize();
    for (size_t i = 0; i < rows; ++i) {
        std::string id = data["cn"].at(i).toString();
        _columns[id].foreignKey = true;
        _columns[id].foreignKeyTable = data["tn"].at(i).toString();;
        _columns[id].foreignKeyColumn = data["fcn"].at(i).toString();;
    }

}

std::vector<RsValue> PgTable::getPrimaryKeys() {
    std::vector<std::string> params;
    params.emplace_back(_schema);
    params.emplace_back(_tableName);
    params.emplace_back("PRIMARY KEY");
    RsData data = _db->query(
            "SELECT t2.column_name FROM information_schema.table_constraints as t1 JOIN "
            "information_schema.constraint_column_usage as t2 ON t1.constraint_name = t2.constraint_name WHERE "
            "t1.table_schema = $1 AND t1.table_name = $2 AND t1.constraint_type = $3",
            params);
    return data["column_name"];
}

RsData PgTable::getAllRows() {
    RsData data = _db->query("SELECT * FROM " + _schema + ".\"" + _tableName + "\"");
    return data;
}

RsData PgTable::getAllRows(const std::string& toAdd) {
    RsData data = _db->query("SELECT * FROM " + _schema + ".\"" + _tableName + "\" " + toAdd);
    return data;
}

std::vector<RsValue> PgTable::getForeignKeys() {
    std::vector<std::string> params;
    params.emplace_back(_schema);
    params.emplace_back(_tableName);
    params.emplace_back("FOREIGN KEY");
    RsData data = _db->query(
            "SELECT t2.column_name FROM information_schema.table_constraints as t1"
            " JOIN information_schema.key_column_usage t2 ON t1.constraint_name = t2.constraint_name"
            " WHERE t1.table_schema = $1 AND"
            " t1.table_name = $2 AND"
            " t1.constraint_type = $3",
            params);
    return data["column_name"];
}

std::vector<RsColumn> PgTable::getColumnsOrdered() {
    std::vector<RsColumn> rtn;
    rtn.reserve(_columns.size());
    for (auto &rsc : getColumns()) {
        rtn.push_back(rsc.second);
    }
    sort(rtn.begin(), rtn.end(), [](const RsColumn &l, const RsColumn &r) {
        return l.ordinalPosition < r.ordinalPosition;
    });
    return rtn;
}

bool PgTable::insert(const std::vector<std::string> &fields, const std::vector<std::string> &values) {
    const auto query = DbUtil::createInsert(_schema, _tableName, fields);
    return _db->execute(query, values);
}

bool PgTable::update(const std::vector<std::string> &fields, const std::vector<std::string> &values, const std::vector<std::string> &wheres, const std::vector<std::string> &whereValues) {
    const auto query = DbUtil::createUpdate(_schema, _tableName, fields, wheres);
    std::vector<std::string> allValues(values.size() + whereValues.size());
    allValues.assign(values.begin(), values.end());
    allValues.insert(allValues.end(), whereValues.begin(), whereValues.end());
    return _db->execute(query, allValues);
}

bool PgTable::deleteWhere(const std::vector<std::string> &wheres,
                          const std::vector<std::string> &whereValues) {
    const auto query = DbUtil::createDelete(_schema, _tableName, wheres);
    return _db->execute(query, whereValues);
}

RsData PgTable::findBy(const std::vector<std::string> &fields, const std::vector<std::string> &wheres,
                       const std::vector<std::string> &values) {
    const auto query = DbUtil::createSelect(_schema, _tableName, fields, wheres);
    return _db->query(query, values);
}