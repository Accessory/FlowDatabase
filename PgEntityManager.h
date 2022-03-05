#pragma once

#include "PgSqlDatabase.h"
#include "PgEntity.h"
#include "DbUtil.h"

class PgEntityManager {
public:
    PgEntityManager(std::shared_ptr<PgSqlDatabase> db) : db(db) {}

    template<class T> findAll(PgEntity entity) {
        const auto columns = entity.GetColumnNames();
        const auto table_name = entity.Name;
        const auto schema_name = entity.Schema;
        const auto sql = DbUtil::createSelect(schema_name, table_name, columns);
        db->

    }

private:
    std::shared_ptr<PgSqlDatabase> db
};