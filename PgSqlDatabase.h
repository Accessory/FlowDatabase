#pragma once


#include <string>
#include <libpq-fe.h>
#include "RsData.h"
#include "PgTable.h"
#include "PgEntity.h"
#include <vector>
#include <FlowUtils/IdleManager.h>
#include <string_view>

class PgTable;

struct PgSqlDatabase_Connection {
    PgSqlDatabase_Connection(PGconn *connection) : connection(connection) {}

    PGconn *connection;
};


class PgSqlDatabase {
public:
    explicit PgSqlDatabase(const std::string &connectionInfo, size_t connectionPoolSize = 15);

    ~PgSqlDatabase();

    RsData query(const std::string &sql);

    RsData query(const std::string &sql, const std::vector<std::string> &params);

    std::string ffQuery(const std::string &sql, const std::vector<std::string> &params);

    std::string ffQuery(const std::string &sql);

    bool execute(const std::string &sql);

    bool execute(const std::string &sql, const std::vector<std::string> &params);

    PgTable getTable(const std::string &tableName);

    PgTable getTable(const std::string &schema, const std::string &tableName);

    std::vector<PgEntity> findAll(const PgEntity &entity);

    std::vector<PgEntity> find(const PgEntity &entity, const std::string &sql, const std::vector<std::string> &params);

    bool insert(const PgEntity &entity);

    void fillPgEntity(const PgEntity &entity, std::vector<PgEntity> &rtn, PGresult *res);

    void fillPgEntity(const PgEntity &entity, std::vector<std::shared_ptr<PgEntity>> &rtn, PGresult *res);

    void copyCSV(const std::string& table, const std::string &file, const std::string &delimiter = ",");

    void copyCSV(const std::string& table, const std::string &file, std::function<void(std::string&)> callback,const std::string &delimiter = "," );

    std::vector<std::shared_ptr<PgEntity>>
    findBy(const PgEntity &entity,
           const std::string &sql,
           const std::vector<std::string> &params);

private:
//    std::mutex _queryMutex;
    bool _isConnected = false;
//    PGconn *_connection;
    IdleManager<PgSqlDatabase_Connection> connections;

    bool isError(PGresult *pResult, PGconn *connection);

    static void fillDataWithRes(RsData &data, PGresult *pResult);

    std::shared_ptr<PgSqlDatabase> self;
};


