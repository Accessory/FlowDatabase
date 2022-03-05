#pragma once

#include <string>
#include <sstream>
#include <vector>

namespace DbUtil {
    inline std::string createPgLogin(const std::string &host, const std::string &port, const std::string &db_name,
                                     const std::string &username, const std::string &password) {
        return "host=" + host + " port=" + port + " dbname=" + db_name + " user=" + username + " password=" +
               password;
    }

    inline std::string createIsIn(const std::string &name, const std::vector<std::string> &one_of) {
        std::stringstream rtn;
        rtn << name << " in ( ";
        for (size_t i = 0; i < one_of.size(); ++i) {
            if (i > 0) {
                rtn << ", ";
            }
            rtn << '\'' << one_of.at(i) << "' ";
        }
        rtn << ")";
        return rtn.str();
    }

    inline std::string
    selectWhereAllIn(const std::string &from, const std::string &from_has, const std::string &from_has_field,
                     const std::vector<std::string> &all_of) {
        std::stringstream rtn;
        rtn << "SELECT f.* FROM "
            << from << " f  WHERE EXISTS (SELECT NULL FROM "
            << from_has << " fh WHERE "
            << createIsIn(from_has_field, all_of) <<
            " AND fh.file_id = f.id\n"
            " GROUP BY fh.file_id\n"
            " HAVING COUNT(DISTINCT fh.name) >= "
            << all_of.size() << ")";
        return rtn.str();
    }

    inline std::string
    createDelete(const std::string &schema, const std::string &table,
                 const std::vector<std::string> &where = std::vector<std::string>()) {
        std::ostringstream in;
        in << "DELETE FROM  ";
        in << schema << ".\"" << table << "\"";

        if (!where.empty()) {
            in << " WHERE ";
            for (size_t i = 0; i < where.size(); ++i) {
                in << "\"" << table << "\"" << '.';
                in << "\"" << where.at(i) << "\" = $" << (i + 1);
                if (i != where.size() - 1) {
                    in << " and";
                }
                in << " ";
            }
        }
        return in.str();
    }

    inline std::string
    createInsert(const std::string &schema, const std::string &table, const std::vector<std::string> &fields) {
        std::ostringstream in;
        in << "INSERT INTO  ";
        in << schema << ".\"" << table << "\" ( ";

        for (size_t i = 0; i < fields.size(); ++i) {
            in << fields.at(i);
            if (i < fields.size() - 1) {
                in << ",";
            }
            in << " ";
        }

        in << ") VALUES ( ";

        for (size_t i = 1; i <= fields.size(); ++i) {
            in << '$' << i;
            if (i != fields.size()) {
                in << ",";
            }
            in << " ";
        }
        in << ")";

        return in.str();
    }

    inline std::string
    createUpdate(const std::string &schema, const std::string &table, const std::vector<std::string> &fields,
                 const std::vector<std::string> &where = std::vector<std::string>()) {
        std::ostringstream in;
        in << "UPDATE " << table;
        in << " SET ";
        for (size_t i = 0; i < fields.size(); ++i) {
//            in << "\"" << table << "\"" << '.';
            in << "\"" << fields.at(i) << "\"";
            in << " = $" << (i + 1);
            if (i < fields.size() - 1) {
                in << ",";
            }
            in << " ";
        }

        if (!where.empty()) {
            in << " WHERE ";
            for (size_t i = 0; i < where.size(); ++i) {
                in << "\"" << table << "\"" << '.';
                in << "\"" << where.at(i) << "\" = $" << (i + fields.size() + 1);
                if (i != where.size() - 1) {
                    in << " and";
                }
                in << " ";
            }
        }
        return in.str();
    }

    inline std::string
    createSelect(const std::string &schema, const std::string &table, const std::vector<std::string> &fields,
                 const std::vector<std::string> &where = std::vector<std::string>()) {
        std::ostringstream in;
        in << "SELECT ";
        for (size_t i = 0; i < fields.size(); ++i) {
            in << "\"" << table << "\"" << '.';
            if (fields.at(i) == "*") {
                in << fields.at(i);
            } else { in << "\"" << fields.at(i) << "\""; }
            if (i < fields.size() - 1) {
                in << ",";
            }
            in << " ";
        }

        in << "FROM " << schema << ".\"" << table << "\"";

        if (!where.empty()) {
            in << " WHERE ";
            for (size_t i = 1; i <= where.size(); ++i) {
                in << "\"" << table << "\"" << '.';
                in << "\"" << where.at(i - 1) << "\" = $" << i;
                if (i != where.size()) {
                    in << " and";
                }
                in << " ";
            }
        }
        return in.str();
    }
}