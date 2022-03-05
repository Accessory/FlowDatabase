#pragma once


#include <string>
#include <sstream>
#include <utility>
#include <vector>
#include <memory>
#include <tuple>

#ifdef DELETE
#undef DELETE
#endif

namespace SqlBuilder {
    enum Type {
        SELECT,
        UPDATE,
        INSERT,
        DELETE
    };

    enum WhereConnector {
        AND, OR, LIKE
    };

    struct Where {
        virtual std::string resolve(std::vector<std::string> &parameters) {
            return "";
        }
    };

    struct Like : public Where {
        Like(std::string column, std::string parameter) : column(std::move(column)), parameter(std::move(parameter)) {}

        Like(std::pair<std::string, std::string> pair) : column(std::move(pair.first)),
                                                         parameter(std::move(pair.second)) {}

        std::string column;
        std::string parameter;

        std::string resolve(std::vector<std::string> &parameters) {
            parameters.emplace_back(parameter);
            return column + " like $" + std::to_string(parameters.size());
        }
    };

    struct ILike : public Where {
        ILike(std::string column, std::string parameter) : column(std::move(column)), parameter(std::move(parameter)) {}

        ILike(std::pair<std::string, std::string> pair) : column(std::move(pair.first)),
                                                          parameter(std::move(pair.second)) {}

        std::string column;
        std::string parameter;

        std::string resolve(std::vector<std::string> &parameters) {
            parameters.emplace_back(parameter);
            return column + " ilike $" + std::to_string(parameters.size());
        }
    };

    struct In : public Where {
        In(std::string column, std::string parameter) : column(std::move(column)), parameter(std::move(parameter)) {}

        In(std::pair<std::string, std::string> pair) : column(std::move(pair.first)),
                                                       parameter(std::move(pair.second)) {}

        std::string column;
        std::string parameter;

        std::string resolve(std::vector<std::string> &parameters) {
            parameters.emplace_back(parameter);
            return column + " in ( $" + std::to_string(parameters.size()) + " )";
        }
    };

    struct In_Val : public Where {
        In_Val(std::string column, std::string parameter) : column(std::move(column)),
                                                            parameter(std::move(parameter)) {}

        In_Val(std::pair<std::string, std::string> pair) : column(std::move(pair.first)),
                                                           parameter(std::move(pair.second)) {}

        std::string column;
        std::string parameter;

        std::string resolve(std::vector<std::string> &parameters) {
            return column + " in ( " + parameter + " )";
        }
    };

    struct Eq : public Where {
        Eq(std::string column, std::string parameter) : column(std::move(column)), parameter(std::move(parameter)) {}

        Eq(std::pair<std::string, std::string> pair) : column(std::move(pair.first)),
                                                       parameter(std::move(pair.second)) {}

        std::string column;
        std::string parameter;

        std::string resolve(std::vector<std::string> &parameters) {
            parameters.emplace_back(parameter);
            return column + " = $" + std::to_string(parameters.size());
        }
    };

    struct EqNot : public Where {
        EqNot(std::string column, std::string parameter) : column(std::move(column)), parameter(std::move(parameter)) {}

        EqNot(std::pair<std::string, std::string> pair) : column(std::move(pair.first)),
                                                          parameter(std::move(pair.second)) {}

        std::string column;
        std::string parameter;

        std::string resolve(std::vector<std::string> &parameters) {
            parameters.emplace_back(parameter);
            return column + " <> $" + std::to_string(parameters.size());
        }
    };

    struct And : public Where {
        And() {}

        And(std::vector<std::shared_ptr<Where>> wheres) : Wheres(std::move(wheres)) {}

        std::vector<std::shared_ptr<Where>> Wheres;

        std::string resolve(std::vector<std::string> &parameters) {
            std::ostringstream rtn;
            for (size_t i = 0; i < Wheres.size(); ++i) {
                if (i != 0) {
                    rtn << " and ";
                } else {
                    rtn << ' ';
                }

                rtn << "( ";
                rtn << Wheres.at(i)->resolve(parameters);
                rtn << " )";

            }

            return rtn.str();
        }
    };

    struct Or : public Where {
        Or() {}

        Or(std::vector<std::shared_ptr<Where>> wheres) : Wheres(std::move(wheres)) {}

        std::vector<std::shared_ptr<Where>> Wheres;

        std::string resolve(std::vector<std::string> &parameters) {
            std::ostringstream rtn;
            for (size_t i = 0; i < Wheres.size(); ++i) {
                if (i != 0) {
                    rtn << " or ";
                } else {
                    rtn << ' ';
                }

                rtn << "( ";
                rtn << Wheres.at(0)->resolve(parameters);
                rtn << " )";

            }

            return rtn.str();
        }
    };

    class SqlBuilder {
    public:
        SqlBuilder() {};

        SqlBuilder(Type type) : type(std::move(type)) {};
        Type type = SELECT;
        std::string table;
        std::string from;
        std::vector<std::tuple<std::string, std::string, std::string>> left_join;
        std::vector<std::string> columns;
        std::vector<std::string> jsonColumns;
        bool jsonAgg = false;
        std::shared_ptr<Where> where;
        std::vector<std::string> groupBy;
        std::vector<std::pair<std::string, std::string>> values;
        std::string orderBy;
        bool desc;
        size_t limit = 0;
        size_t offset = 0;

        std::string build() {
            std::vector<std::string> parameters;
            return build(parameters);
        }

        std::string build(std::vector<std::string> &parameters) {
            std::ostringstream rtn;

            switch (type) {
                case SELECT:
                    rtn << "SELECT ";
                    for (size_t i = 0; i < columns.size(); ++i) {
//                        rtn << "\"" << table << "\"" << '.';
                        if (columns.at(i) == "*") {
                            rtn << columns.at(i);
                        } else { rtn << columns.at(i); }
                        if (i < columns.size() - 1) {
                            rtn << ",";
                        }
                        rtn << " ";
                    }
                    if (!jsonColumns.empty()) {
                        if (jsonAgg) {
                            rtn << "json_agg( ";
                        } else {
                            rtn << "to_json( ";
                        }
                    }
                    for (size_t i = 0; i < jsonColumns.size(); ++i) {
                        rtn << table << '.';
                        if (jsonColumns.at(i) == "*") {
                            rtn << jsonColumns.at(i);
                        } else { rtn << jsonColumns.at(i); }
                        if (i < jsonColumns.size() - 1) {
                            rtn << ", ";
                        } else {
                            rtn << " ) ";
                        }
                    }

                    rtn << "FROM " << (from.empty() ? table : from);
                    if (!left_join.empty()) {
                        for (size_t i = 0; i < left_join.size(); ++i) {
                            rtn << " LEFT JOIN ";
                            rtn << std::get<0>(left_join.at(i));
                            rtn << " ON ";
                            rtn << std::get<1>(left_join.at(i));
                            rtn << " = ";
                            rtn << std::get<2>(left_join.at(i));
                        }
                    }

                    break;
                case UPDATE:
                    rtn << "UPDATE " << table;
                    rtn << " SET ";
                    for (size_t i = 0; i < values.size(); ++i) {
                        parameters.emplace_back(values.at(i).first);
                        rtn << "\"" << values.at(i).second << "\"";
                        rtn << " = $" << parameters.size();
                        if (i < values.size() - 1) {
                            rtn << ",";
                        }
                        rtn << ' ';
                    }
                    break;
                case INSERT:
                    rtn << "INSERT INTO  ";
                    rtn << table << " ( ";

                    for (size_t i = 0; i < values.size(); ++i) {
                        rtn << values.at(i).first;
                        if (i < values.size() - 1) {
                            rtn << ",";
                        }
                        rtn << " ";
                    }

                    rtn << ") VALUES ( ";

                    for (size_t i = 0; i < values.size(); ++i) {
                        parameters.emplace_back(values.at(i).second);
                        rtn << '$' << parameters.size();
                        if (i != (values.size() - 1)) {
                            rtn << ",";
                        }
                        rtn << " ";
                    }
                    rtn << ")";
                    return rtn.str();
                case DELETE:
                    rtn << "DELETE FROM  ";
                    rtn << table;
                    break;
            }


            if (where != nullptr) {
                const auto tmp = where->resolve(parameters);
                if (!tmp.empty()) {
                    rtn << " WHERE " << tmp;
                }
            }

            if (type != SELECT)
                return rtn.str();


            if (!groupBy.empty()) {
                rtn << " GROUP BY";
                for (size_t i = 0; i < groupBy.size(); ++i) {
                    if (i != 0) {
                        rtn << ", ";
                    } else {
                        rtn << " ";
                    }
                    rtn << groupBy.at(0);
                }
            }

            if (!orderBy.empty()) {
                rtn << " ORDER BY " << orderBy;

                if (desc) {
                    rtn << " DESC";
                } else {
                    rtn << " ASC";
                }
            }

            if (limit != 0) {
                rtn << " LIMIT " << limit;
            }

            if (offset != 0) {
                rtn << " OFFSET " << offset;
            }

            return rtn.str();
        }
    };

}

