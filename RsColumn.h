#pragma once

#include <string>

class RsColumn {
public:
    RsColumn(){}
    RsColumn(const std::string &name) : name(name), type("TEXT") {}

    RsColumn(const std::string &name, const std::string &type, bool primaryKey = false) : name(name), type(type) {}

    RsColumn(const std::string &name, const std::string &type, bool null, bool unique, bool primaryKey) : name(name),
                                                                                                          type(type),
                                                                                                          null(null),
                                                                                                          unique(unique),
                                                                                                          primaryKey(
                                                                                                                  primaryKey) {}

    RsColumn(const std::string &name, const std::string &type, bool null, bool unique,
             const std::string &foreignKeyTable, const std::string &foreignKeyColumn) : name(name), type(type),
                                                                                        null(null), unique(unique),
                                                                                        foreignKeyTable(
                                                                                                foreignKeyTable),
                                                                                        foreignKeyColumn(
                                                                                                foreignKeyColumn),
                                                                                        foreignKey(true){
    }


    std::string name;
    std::string type;
    bool null = false;
    bool unique = false;
    bool primaryKey = false;
    bool foreignKey = false;
    std::string foreignKeyTable = "";
    std::string foreignKeyColumn = "";
    size_t ordinalPosition;
};


