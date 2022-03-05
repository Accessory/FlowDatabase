#pragma once

#include <string>
#include <unordered_map>
#include "FlowDatabase/PgDatatypes/PgColumnBase.h"
#include <vector>
#include <memory>
#include <utility>
#include <tuple>
#include "PgEntityVector.h"

class PgEntity : public PgEntityVector {
public:
    PgEntity(std::string name, std::string schema = "public") : Name(std::move(name)), Schema(std::move(schema)) {
    }

    std::string Name;
    std::string Schema = "public";
    std::shared_ptr<PgColumnBase> PrimaryKey;

    std::shared_ptr<PgColumnBase> Get(std::string_view columnName) {
        for(auto column : *this) {
            return column;
        }
        return nullptr;
    }

    template<class T>
    T &Get() {
        return reinterpret_cast<T &>(*this);
    };

    std::vector<std::string> GetColumnNames() const {
        std::vector<std::string> rtn;
        for (const auto &column : *this) {
            if (!column->IsRef) {
                rtn.emplace_back(column->Name);
            }
        }
        return rtn;
    }

    std::vector<std::string> GetColumnNamesWithTable() const {
        std::vector<std::string> rtn;
        for (const auto &column : *this) {
            if (!column->IsRef) {
                rtn.emplace_back(Name + '.' + column->Name);
            }
        }
        return rtn;
    }

    size_t GetNonRefSize() const {
        size_t rtn = 0;
        for (const auto &column : *this) {
            if (!column->IsRef) {
                ++rtn;
            }
        }
        return rtn;
    }

    std::vector<std::pair<std::string, std::string>> GetPairs() const {
        std::vector<std::pair<std::string, std::string>> rtn;

        for (const auto &column : *this) {
            if (column->IsRef) { continue; }
            std::pair<std::string, std::string> value = {column->Name, column->ToString()};
            rtn.emplace_back(value);
        }

        return rtn;
    }

    std::vector<std::pair<std::string, std::string>> GetDirtyPairs() const {
        std::vector<std::pair<std::string, std::string>> rtn;

        for (const auto &column : *this) {
            std::pair<std::string, std::string> value = {column->Name, column->ToString()};
            rtn.emplace_back(std::move(value));
        }

        return rtn;
    }

    std::pair<std::string, std::string> GetPrimaryKeyPair() const {
        return {PrimaryKey->Name, PrimaryKey->ToString()};
    }

    std::string GetSchemaTable() const {
        return Schema + ".\"" + Name + "\"";
    }

    std::string ToArrayArg() const {
        return "array_agg(" + Name + ".*)";
    }




    virtual PgEntity New() const {
        return PgEntity(Name, Schema);
    }
    virtual PgEntity* NewPtr() const {
        return new PgEntity(Name, Schema);
    }
};