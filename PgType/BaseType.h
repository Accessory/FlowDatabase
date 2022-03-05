#pragma once

class PGType{
public:
    std::string name;
    std::string type;
    bool isNull = false;
    bool isPrimaryKey = false;
};