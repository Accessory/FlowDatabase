#pragma once

#include "../PgEntity.h"
#include "PgJoin.h"

namespace PgUtil {
    inline std::tuple<std::string, std::string, std::string> makeJoinTuple(const PgEntity& entity, const std::string_view columnName) {
        for (const auto &column : entity) {
            if (column->Name != columnName)
                continue;

            const auto table = column->Name;
            const auto on = entity.Name + '.' + column->Get<PgJoin>().On;
            const auto onTo = table + '.' + column->Get<PgJoin>().OnTo;

            return std::tuple(table, on, onTo);
        }
        const auto on_table = std::string(columnName);
        return std::tuple(on_table, entity.Name + ".id", on_table + "." + entity.Name + "_id");
    }

};
