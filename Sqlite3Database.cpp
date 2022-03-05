#include <FlowUtils/FlowLog.h>
#include "Sqlite3Database.h"
#include "RsValue.h"
#include "S3Table.h"

Sqlite3Database::Sqlite3Database(const std::string &database) {

    auto rc = sqlite3_open(database.c_str(), &_db);
    if (rc) {
        LOG_ERROR << "Can't open database: %s";
    }
    _isOpen = true;
    LOG_INFO << "Open Database: " << database;
}


Sqlite3Database::~Sqlite3Database() {
    if (_isOpen)
        sqlite3_close(_db);
    _isOpen = false;
}

bool Sqlite3Database::execute(const std::string &sql) {
    char *errorMessage = nullptr;
    auto rc = sqlite3_exec(_db, sql.c_str(), nullptr, nullptr, &errorMessage);
    if (checkError(rc, errorMessage, sql)) {
        sqlite3_free(errorMessage);
        return false;
    }
    return true;
}

bool Sqlite3Database::clear(const std::string &table_name) {
    std::string sql = "DELETE from " + table_name;
    return execute(sql);
}

bool Sqlite3Database::insert(const std::string &table_name, const std::map<std::string, std::string> &values) {
    std::stringstream sql;
    std::stringstream sql_names;
    std::stringstream sql_values;
    sql << "insert into " << table_name << "(";
    auto i = values.size();
    auto c = 1;
    for (auto &value : values) {
        sql_names << '\'' << value.first << '\'';
        sql_values << "?" << c++;
        if (--i != 0) {
            sql_names << ", ";
            sql_values << ", ";
        }
    }
    sql << sql_names.str() << ") values (" << sql_values.str() << ");";

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(_db, sql.str().c_str(), -1, &stmt, nullptr);
    c = 1;
    for (auto &value : values) {
        sqlite3_bind_text(stmt, c++, value.second.c_str(), value.second.size(), SQLITE_STATIC);
    }


    auto rc = sqlite3_step(stmt);
    if (checkError(rc))
        return false;
    return true;
}

bool Sqlite3Database::update(const std::string &table_name, const std::string &key,
                             const std::map<std::string, std::string> &values) {
    std::stringstream sql;
    std::stringstream sql_names;
    std::stringstream sql_values;
    sql << "Update " << table_name << " SET ";
    auto c = 1;
    for (auto &value : values) {
        if (key != value.first)
            sql << '"' << value.first << '"' << '=' << ++c << ' ';
    }
    sql << "WHERE \"" << key << "\"" << values.at(key);

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(_db, sql.str().c_str(), -1, &stmt, nullptr);
    c = 1;
    for (auto &value : values) {
        sqlite3_bind_text(stmt, c++, value.second.c_str(), value.second.size(), SQLITE_STATIC);
    }


    auto rc = sqlite3_step(stmt);
    if (checkError(rc))
        return false;
    return true;
}

RsData Sqlite3Database::query(const std::string &table_name, const std::vector<std::string> &columns,
                              const std::map<std::string, std::string> whereEquals) {
    char *errorMessage = nullptr;
    RsData data;
    std::string columns_string;
    if (columns.empty()) {
        columns_string = "*";
    } else {
        size_t elementsLeft = columns.size();
        for (const auto &column : columns) {
            columns_string += column;
            if (--elementsLeft != 0)
                columns_string += " ,";
        }
    }
    std::string whereClause;
    if (whereEquals.empty()) {
        whereClause = ";";
    } else {
        whereClause = " WHERE ";
        size_t elementsLeft = whereEquals.size();
        for (const auto pair : whereEquals) {
            whereClause += pair.first + " = '" + pair.second + "'";
            if (--elementsLeft != 0)
                whereClause += " and ";
        }
    }

    std::string sql = "SELECT " + columns_string + " FROM " + table_name + whereClause;

    return query(sql);
}

RsData Sqlite3Database::query(const std::string &qry) {
    char *errorMessage = nullptr;
    RsData data;


    int rc = sqlite3_exec(_db, qry.c_str(), &Sqlite3Database::readingCallback, static_cast<void *>(&data),
                          &errorMessage);
    if (checkError(rc, errorMessage, qry)) {
        sqlite3_free(errorMessage);
        return data;
    }

    return data;
}

RsData Sqlite3Database::getData(sqlite3_stmt *stmt, const std::string &qry) {
    RsData data;
    char *errorMessage = nullptr;
    auto column_count = sqlite3_column_count(stmt);
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        for (int i = 0; i < column_count; ++i) {
            std::string columnName(sqlite3_column_name(stmt, i));
            auto itr = data.find(columnName);
            if (itr == data.end()) {
                itr = data.insert(make_pair(columnName, std::vector<RsValue>())).first;
            }
            auto column_data_sqlite3 = sqlite3_column_text(stmt, i);
            auto column_data_size_sqlite3 = sqlite3_column_bytes(stmt, i);
            itr->second.emplace_back(column_data_sqlite3, column_data_size_sqlite3);

        }
    }

    if (checkError(rc, errorMessage, qry)) {
        sqlite3_free(errorMessage);
        return data;
    }
    return data;
}

RsData Sqlite3Database::query(const std::string &qry, const std::vector<std::string> &values) {


    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(_db, qry.c_str(), -1, &stmt, nullptr);
    auto c = 1;
    for (const auto &value : values) {
        sqlite3_bind_text(stmt, c++, value.c_str(), value.size(), SQLITE_STATIC);
    }

    RsData data = getData(stmt, qry);

    return data;
}

bool Sqlite3Database::exists(const std::string &table_name, const std::string &column, const std::string &value) {
    std::string sql = "SELECT COUNT(*) as rc FROM " + table_name + " WHERE " + column + " = ?;";
    return this->query(sql, {value})["rc"].front().toBoolean();
}

bool Sqlite3Database::checkError(int rc, char *errorMessage, const std::string &sql) {
    if (rc && rc != SQLITE_DONE) {
        LOG_ERROR << "Sql Fail: " << errorMessage << std::endl << sql;
        return true;
    }

    return false;
}

bool Sqlite3Database::checkError(int rc, char *errorMessage) {
    if (rc) {
        LOG_ERROR << "Sql Fail: " << errorMessage;
        return true;
    }

    return false;
}

bool Sqlite3Database::checkError(int rc) {
    if (rc == SQLITE_OK || rc == SQLITE_DONE) {
        return false;
    }
    LOG_ERROR << "Sql Fail: " << sqlite3_errmsg(_db);
    return true;
}

std::string Sqlite3Database::getLastError() {
    return std::string(sqlite3_errmsg(_db));
}

int Sqlite3Database::readingCallback(void *data, int argc, char **argv, char **columnNames) {
    RsData *rsData = static_cast<RsData *>(data);
    for (int i = 0; i < argc; ++i) {
        std::string columnName(columnNames[i]);
        auto itr = rsData->find(columnName);
        if (itr == rsData->end()) {
            itr = (*rsData).insert(make_pair(columnName, std::vector<RsValue>())).first;
        }
        itr->second.emplace_back(argv[i]);

    }
    return 0;
}

std::vector<unsigned char> Sqlite3Database::getBlob(const std::string &sql) {
    LOG_DEBUG << sql;
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(_db, sql.c_str(), -1, &stmt, nullptr);
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        LOG_ERROR << "Error: " << sqlite3_errmsg(_db);
        sqlite3_finalize(stmt);
        return std::vector<unsigned char>();
    }
    int size = sqlite3_column_bytes(stmt, 0);
    unsigned char *dataBlob = (unsigned char *) sqlite3_column_blob(stmt, 0);
    std::vector<unsigned char> rtn(dataBlob, dataBlob + size);

    sqlite3_finalize(stmt);

    return rtn;
}

S3Table Sqlite3Database::getTable(const std::string &tableName) {
    return S3Table(this, tableName);
}

bool Sqlite3Database::deleteRows(const std::string &table_name, const std::map<std::string, std::string> &values) {
    std::stringstream sql;
    std::stringstream sql_values;
    sql << "DELETE FROM " << table_name;
    auto i = values.size();
    auto c = 1;
    for (auto &value : values) {
        sql_values << value.first << " = ?" << c++;
        if (--i != 0) {
            sql_values << ", ";
        }
    }
    sql << " WHERE " << sql_values.str() << ";";

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(_db, sql.str().c_str(), -1, &stmt, nullptr);
    c = 1;
    for (auto &value : values) {
        sqlite3_bind_text(stmt, c++, value.second.c_str(), value.second.size(), SQLITE_STATIC);
    }


    auto rc = sqlite3_step(stmt);
    if (checkError(rc))
        return false;
    return true;
}



