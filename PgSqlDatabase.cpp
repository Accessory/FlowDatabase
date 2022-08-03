#include "PgSqlDatabase.h"
#include "SqlBuilder/SqlBuilder.h"
#include <FlowUtils/FlowLog.h>
#include <FlowUtils/FlowString.h>
#include <FlowUtils/IdleManager.h>
#include <FlowUtils/StringFileHandler.h>
#include <memory>
#include <string>

namespace PgSqlDatabase_Deleter {
template <typename T, typename Deleter = typename std::default_delete<T>>
struct DisarmedDelete : private Deleter {
  void operator()(T *ptr) {
    if (_armed)
      Deleter::operator()(ptr);
  }

  bool _armed = false;
};
} // namespace PgSqlDatabase_Deleter

PgSqlDatabase::PgSqlDatabase(const std::string &connectionInfo,
                             size_t connectionPoolSize) {
  //    _connection = PQconnectdb(connectionInfo.c_str());
  for (size_t i = 0; i < connectionPoolSize; ++i) {
    PGconn *new_connection = PQconnectdb(connectionInfo.c_str());
    if (PQstatus(new_connection) != CONNECTION_OK) {
      LOG_ERROR << "Failed to connect to: " << connectionInfo;
      LOG_ERROR << "Error Message: " << PQerrorMessage(new_connection);
    }
    connections.add(std::make_shared<PgSqlDatabase_Connection>(new_connection));
  }
  this->self = std::shared_ptr<PgSqlDatabase>(
      this, PgSqlDatabase_Deleter::DisarmedDelete<PgSqlDatabase>());
  _isConnected = true;
}

PgSqlDatabase::~PgSqlDatabase() {
  //    if(this->self) {
  //        auto deleter =
  //        get_deleter<PgSqlDatabase_Deleter::DisarmedDelete<PgSqlDatabase>>(this->self);
  //    }
  auto items = connections.releaseAll();
  for (auto &item : items) {
    PQfinish(item->connection);
  }
  if (_isConnected)
    //        PQfinish(_connection);
    _isConnected = false;
}

RsData PgSqlDatabase::query(const std::string &sql) {
  LOG_DEBUG << "Query: " << sql;
  //    std::lock_guard<std::mutex> query_lock(_queryMutex);
  auto connection = connections.get();
  PGresult *res = PQexec(connection->object->connection, sql.c_str());

  RsData data;

  if (isError(res, connection->object->connection))
    return data;
  fillDataWithRes(data, res);
  PQclear(res);
  return data;
}

RsData PgSqlDatabase::query(const std::string &sql,
                            const std::vector<std::string> &params) {
  //    std::lock_guard<std::mutex> query_lock(_queryMutex);
  LOG_DEBUG << "Query: " << sql;
  RsData data;
  const size_t paramsSize = params.size();
  //    const char *paramChars[paramsSize];
  //    int paramLengths[paramsSize];
  //    int paramFormats[paramsSize];

  std::vector<const char *> paramChars;
  std::vector<int> paramLengths;
  std::vector<int> paramFormats;

  for (size_t i = 0; i < paramsSize; ++i) {
    LOG_TRACE << "Parameter " << i << ": " << params.at(i);
    paramChars.push_back(params.at(i).c_str());
    paramLengths.push_back(static_cast<int>(params.at(i).size()));
    paramFormats.push_back(0); // Insert Text
  }
  auto connection = connections.get();
  PGresult *res = PQexecParams(
      connection->object->connection, sql.c_str(), static_cast<int>(paramsSize),
      NULL, /* let the backend deduce param type */
      paramChars.data(), paramLengths.data(), paramFormats.data(),
      0); /* ask for text results */

  if (isError(res, connection->object.get()->connection))
    return data;
  fillDataWithRes(data, res);
  PQclear(res);
  return data;
}

RsData PgSqlDatabase::query(
    const std::string &sql, const std::vector<std::string> &params,
    std::shared_ptr<IdleObject<PgSqlDatabase_Connection>> connection) {
  //    std::lock_guard<std::mutex> query_lock(_queryMutex);
  LOG_DEBUG << "Query: " << sql;
  RsData data;
  const size_t paramsSize = params.size();
  //    const char *paramChars[paramsSize];
  //    int paramLengths[paramsSize];
  //    int paramFormats[paramsSize];

  std::vector<const char *> paramChars;
  std::vector<int> paramLengths;
  std::vector<int> paramFormats;

  for (size_t i = 0; i < paramsSize; ++i) {
    LOG_TRACE << "Parameter " << i << ": " << params.at(i);
    paramChars.push_back(params.at(i).c_str());
    paramLengths.push_back(static_cast<int>(params.at(i).size()));
    paramFormats.push_back(0); // Insert Text
  }

  PGresult *res = PQexecParams(
      connection->object->connection, sql.c_str(), static_cast<int>(paramsSize),
      NULL, /* let the backend deduce param type */
      paramChars.data(), paramLengths.data(), paramFormats.data(),
      0); /* ask for text results */

  if (isError(res, connection->object.get()->connection))
    return data;
  fillDataWithRes(data, res);
  PQclear(res);
  return data;
}

std::string PgSqlDatabase::ffQuery(const std::string &sql) {
  LOG_DEBUG << "Query: " << sql;
  auto connection = connections.get();

  PGresult *res = PQexec(connection->object->connection, sql.c_str());

  std::string rtn;
  if (isError(res, connection->object.get()->connection))
    return rtn;

  if (PQresultStatus(res) == PGRES_TUPLES_OK) {
    rtn = std::string(PQgetvalue(res, 0, 0), PQgetlength(res, 0, 0));
  }

  PQclear(res);
  return rtn;
}

std::string PgSqlDatabase::ffQuery(const std::string &sql,
    std::shared_ptr<IdleObject<PgSqlDatabase_Connection>> connection) {
  LOG_DEBUG << "Query: " << sql;

  PGresult *res = PQexec(connection->object->connection, sql.c_str());

  std::string rtn;
  if (isError(res, connection->object.get()->connection))
    return rtn;

  if (PQresultStatus(res) == PGRES_TUPLES_OK) {
    rtn = std::string(PQgetvalue(res, 0, 0), PQgetlength(res, 0, 0));
  }

  PQclear(res);
  return rtn;
}

std::string PgSqlDatabase::ffQuery(
    const std::string &sql, const std::vector<std::string> &params,
    std::shared_ptr<IdleObject<PgSqlDatabase_Connection>> connection) {
  LOG_DEBUG << "Query: " << sql;
  const size_t paramsSize = params.size();
  std::vector<const char *> paramChars;
  std::vector<int> paramLengths;
  std::vector<int> paramFormats;

  for (size_t i = 0; i < paramsSize; ++i) {
    LOG_TRACE << "Parameter " << i << ": " << params.at(i);
    paramChars.push_back(params.at(i).c_str());
    paramLengths.push_back(static_cast<int>(params.at(i).size()));
    paramFormats.push_back(0); // Insert Text
  }
  PGresult *res = PQexecParams(
      connection->object->connection, sql.c_str(), static_cast<int>(paramsSize),
      NULL, /* let the backend deduce param type */
      paramChars.data(), paramLengths.data(), paramFormats.data(),
      0); /* ask for text results */

  std::string rtn;
  if (isError(res, connection->object.get()->connection))
    return rtn;

  if (PQresultStatus(res) == PGRES_TUPLES_OK) {
    rtn = std::string(PQgetvalue(res, 0, 0), PQgetlength(res, 0, 0));
  }

  PQclear(res);
  return rtn;
}

std::string PgSqlDatabase::ffQuery(const std::string &sql,
                                   const std::vector<std::string> &params) {
  LOG_DEBUG << "Query: " << sql;
  const size_t paramsSize = params.size();
  std::vector<const char *> paramChars;
  std::vector<int> paramLengths;
  std::vector<int> paramFormats;

  for (size_t i = 0; i < paramsSize; ++i) {
    LOG_TRACE << "Parameter " << i << ": " << params.at(i);
    paramChars.push_back(params.at(i).c_str());
    paramLengths.push_back(static_cast<int>(params.at(i).size()));
    paramFormats.push_back(0); // Insert Text
  }
  auto connection = connections.get();
  PGresult *res = PQexecParams(
      connection->object->connection, sql.c_str(), static_cast<int>(paramsSize),
      NULL, /* let the backend deduce param type */
      paramChars.data(), paramLengths.data(), paramFormats.data(),
      0); /* ask for text results */

  std::string rtn;
  if (isError(res, connection->object.get()->connection))
    return rtn;

  if (PQresultStatus(res) == PGRES_TUPLES_OK) {
    rtn = std::string(PQgetvalue(res, 0, 0), PQgetlength(res, 0, 0));
  }

  PQclear(res);
  return rtn;
}

bool PgSqlDatabase::isError(PGresult *res, PGconn *connection) {
  ExecStatusType result = PQresultStatus(res);
  if (result == PGRES_BAD_RESPONSE || result == PGRES_FATAL_ERROR) {
    LOG_ERROR << "Error Message: " << PQerrorMessage(connection);
    PQclear(res);
    return true;
  }

  return false;
}

void PgSqlDatabase::fillDataWithRes(RsData &data, PGresult *res) {
  if (PQresultStatus(res) == PGRES_TUPLES_OK) {
    int fieldsCount = PQnfields(res);
    int resultCount = PQntuples(res);
    for (int row = 0; row < resultCount; ++row) {
      for (int col = 0; col < fieldsCount; ++col) {
        std::string columnName(PQfname(res, col));
        auto itr = data.find(columnName);
        if (itr == data.end()) {
          itr =
              data.insert(make_pair(columnName, std::vector<RsValue>())).first;
        }
        itr->second.emplace_back(PQgetvalue(res, row, col),
                                 PQgetlength(res, row, col));
      }
    }
  }
}

PgTable PgSqlDatabase::getTable(const std::string &tableName) {
  return PgTable(this->self, "public", tableName);
}

bool PgSqlDatabase::hasTable(const std::string &schema,
                             const std::string &tableName) {
  const std::vector<std::string> params = {schema, tableName};
  const auto result =
      this->ffQuery("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = "
                    "$1 AND tablename = $2);",
                    params);
  return result == "t";
}

PgTable PgSqlDatabase::getTable(const std::string &schema,
                                const std::string &tableName) {
  return PgTable(this->self, schema, tableName);
}

bool PgSqlDatabase::execute(const std::string &sql,
                            const std::vector<std::string> &params) {
  LOG_DEBUG << "Execute: " << sql;
  const size_t paramsSize = params.size();
  std::vector<const char *> paramChars;
  std::vector<int> paramLengths;
  std::vector<int> paramFormats;

  for (size_t i = 0; i < paramsSize; ++i) {
    LOG_TRACE << "Parameter " << i << ": " << params.at(i);
    paramChars.push_back(params.at(i).c_str());
    paramLengths.push_back(static_cast<int>(params.at(i).size()));
    paramFormats.push_back(0); // Insert Text
  }
  auto connection = connections.get();
  PGresult *res = PQexecParams(
      connection->object->connection, sql.c_str(), static_cast<int>(paramsSize),
      NULL, /* let the backend deduce param type */
      paramChars.data(), paramLengths.data(), paramFormats.data(),
      0); /* ask for text results */

  if (PQresultStatus(res) != PGRES_COMMAND_OK &&
      PQresultStatus(res) != PGRES_TUPLES_OK) {
    LOG_ERROR << PQerrorMessage(connection->object->connection);
    PQclear(res);
    return false;
  }
  PQclear(res);
  return true;
}

bool PgSqlDatabase::execute(
    const std::string &sql, const std::vector<std::string> &params,
    std::shared_ptr<IdleObject<PgSqlDatabase_Connection>> connection) {
  LOG_DEBUG << "Execute: " << sql;
  const size_t paramsSize = params.size();
  std::vector<const char *> paramChars;
  std::vector<int> paramLengths;
  std::vector<int> paramFormats;

  for (size_t i = 0; i < paramsSize; ++i) {
    LOG_TRACE << "Parameter " << i << ": " << params.at(i);
    paramChars.push_back(params.at(i).c_str());
    paramLengths.push_back(static_cast<int>(params.at(i).size()));
    paramFormats.push_back(0); // Insert Text
  }
  PGresult *res = PQexecParams(
      connection->object->connection, sql.c_str(), static_cast<int>(paramsSize),
      NULL, /* let the backend deduce param type */
      paramChars.data(), paramLengths.data(), paramFormats.data(),
      0); /* ask for text results */

  if (PQresultStatus(res) != PGRES_COMMAND_OK &&
      PQresultStatus(res) != PGRES_TUPLES_OK) {
    LOG_ERROR << PQerrorMessage(connection->object->connection);
    PQclear(res);
    return false;
  }
  PQclear(res);
  return true;
}

bool PgSqlDatabase::execute(const std::string &sql) {
  LOG_DEBUG << sql;
  auto connection = connections.get();
  PGresult *res = PQexec(connection->object->connection, sql.c_str());

  if (PQresultStatus(res) != PGRES_COMMAND_OK &&
      PQresultStatus(res) != PGRES_TUPLES_OK) {
    LOG_ERROR << PQerrorMessage(connection->object->connection);
    PQclear(res);
    return false;
  }

  PQclear(res);
  return true;
}

bool PgSqlDatabase::execute(const std::string &sql,
    std::shared_ptr<IdleObject<PgSqlDatabase_Connection>> connection) {
  LOG_DEBUG << sql;
  PGresult *res = PQexec(connection->object->connection, sql.c_str());

  if (PQresultStatus(res) != PGRES_COMMAND_OK &&
      PQresultStatus(res) != PGRES_TUPLES_OK) {
    LOG_ERROR << PQerrorMessage(connection->object->connection);
    PQclear(res);
    return false;
  }

  PQclear(res);
  return true;
}

std::vector<PgEntity>
PgSqlDatabase::find(const PgEntity &entity, const std::string &sql,
                    const std::vector<std::string> &params) {
  LOG_DEBUG << "Query: " << sql;
  std::vector<PgEntity> data;
  const size_t paramsSize = params.size();

  std::vector<const char *> paramChars;
  std::vector<int> paramLengths;
  std::vector<int> paramFormats;

  for (size_t i = 0; i < paramsSize; ++i) {
    LOG_TRACE << "Parameter " << i << ": " << params.at(i);
    paramChars.push_back(params.at(i).c_str());
    paramLengths.push_back(static_cast<int>(params.at(i).size()));
    paramFormats.push_back(0); // Insert Text
  }
  auto connection = connections.get();
  PGresult *res = PQexecParams(
      connection->object->connection, sql.c_str(), static_cast<int>(paramsSize),
      NULL, /* let the backend deduce param type */
      paramChars.data(), paramLengths.data(), paramFormats.data(),
      0); /* ask for text results */

  if (isError(res, connection->object.get()->connection))
    return data;

  fillPgEntity(entity, data, res);

  PQclear(res);
  return data;
}

std::vector<std::shared_ptr<PgEntity>>
PgSqlDatabase::findBy(const PgEntity &entity, const std::string &sql,
                      const std::vector<std::string> &params) {
  LOG_DEBUG << "Query: " << sql;
  std::vector<std::shared_ptr<PgEntity>> data;
  const size_t paramsSize = params.size();

  std::vector<const char *> paramChars;
  std::vector<int> paramLengths;
  std::vector<int> paramFormats;

  for (size_t i = 0; i < paramsSize; ++i) {
    LOG_TRACE << "Parameter " << i << ": " << params.at(i);
    paramChars.push_back(params.at(i).c_str());
    paramLengths.push_back(static_cast<int>(params.at(i).size()));
    paramFormats.push_back(0); // Insert Text
  }
  auto connection = connections.get();
  PGresult *res = PQexecParams(
      connection->object->connection, sql.c_str(), static_cast<int>(paramsSize),
      NULL, /* let the backend deduce param type */
      paramChars.data(), paramLengths.data(), paramFormats.data(),
      0); /* ask for text results */

  if (isError(res, connection->object.get()->connection))
    return data;

  fillPgEntity(entity, data, res);

  PQclear(res);
  return data;
}

void PgSqlDatabase::fillPgEntity(const PgEntity &entity,
                                 std::vector<PgEntity> &rtn, PGresult *res) {
  if (PQresultStatus(res) == PGRES_TUPLES_OK) {
    int fieldsCount = PQnfields(res);
    int resultCount = PQntuples(res);
    for (size_t row = 0; row < resultCount; ++row) {
      PgEntity new_entity = entity.New();
      for (int col = 0; col < fieldsCount; ++col) {
        new_entity.at(col)->Get<PgColumnBase>().Set(PQgetvalue(res, row, col),
                                                    PQgetlength(res, row, col));
        new_entity.at(col)->Get<PgColumnBase>().Dirty = false;
      }
      rtn.emplace_back(std::move(new_entity));
    }
  }
}

void PgSqlDatabase::fillPgEntity(const PgEntity &entity,
                                 std::vector<std::shared_ptr<PgEntity>> &rtn,
                                 PGresult *res) {
  if (PQresultStatus(res) == PGRES_TUPLES_OK) {
    int fieldsCount = PQnfields(res);
    int resultCount = PQntuples(res);
    for (size_t row = 0; row < resultCount; ++row) {
      std::shared_ptr<PgEntity> new_entity(entity.NewPtr());
      for (int col = 0; col < fieldsCount; ++col) {
        new_entity->at(col)->Get<PgColumnBase>().Set(
            PQgetvalue(res, row, col), PQgetlength(res, row, col));
        new_entity->at(col)->Get<PgColumnBase>().Dirty = false;
      }
      rtn.emplace_back(new_entity);
    }
  }
}

std::vector<PgEntity> PgSqlDatabase::findAll(const PgEntity &entity) {
  std::vector<PgEntity> data;
  SqlBuilder::SqlBuilder builder;
  builder.table = entity.GetSchemaTable();
  builder.columns = entity.GetColumnNames();
  const auto sql = builder.build();
  LOG_DEBUG << sql;
  auto connection = connections.get();
  PGresult *res = PQexec(connection->object->connection, sql.c_str());

  if (isError(res, connection->object.get()->connection))
    return data;

  fillPgEntity(entity, data, res);

  return data;
}

bool PgSqlDatabase::insert(const PgEntity &entity) {
  SqlBuilder::SqlBuilder builder;
  builder.type = SqlBuilder::INSERT;
  builder.table = entity.GetSchemaTable();
  builder.values = entity.GetPairs();
  std::vector<std::string> parameters;
  const auto sql = builder.build(parameters);
  LOG_DEBUG << sql;
  return execute(sql, parameters);
}

void PgSqlDatabase::copyCSV(const std::string &table, const std::string &file,
                            std::function<void(std::string &)> callback,
                            const std::string &delimiter) {
  StringFileHandler sfh(file);
  if (!sfh.HasNext()) {
    LOG_ERROR << "Empty file";
  }
  const auto header = sfh.GetLine();

  const auto columns = FlowString::splitNotEmpty(header, delimiter);
  std::ostringstream sql;
  sql << "COPY " << table << "(";

  for (size_t i = 0; i < columns.size(); ++i) {
    sql << columns.at(i);
    if (i != (columns.size() - 1)) {
      sql << ", ";
    }
  }

  sql << ") FROM STDIN CSV DELIMITER '" << delimiter << "';";

  auto connection = connections.get();
  const auto res = PQexec(connection->object->connection, sql.str().c_str());

  if (isError(res, connection->object->connection) ||
      PQresultStatus(res) != PGRES_COPY_IN) {
    return;
  }

  int a = 1;

  while (sfh.HasNext() && a == 1) {
    auto buffer = sfh.GetLine() + '\n';
    callback(buffer);
    if (buffer.size() > 1) {
      a = PQputCopyData(connection->object->connection, buffer.c_str(),
                        buffer.size());
    }
  }
  const char *errmsg = nullptr;
  PQputCopyEnd(connection->object->connection, errmsg);

  const auto res_cp = PQgetResult(connection->object->connection);
  if (isError(res_cp, connection->object->connection)) {
    return;
  }

  PQclear(res);
  PQclear(res_cp);
  LOG_INFO << "Finished: " << sql.str();
}

void PgSqlDatabase::copyCSV(const std::string &table, const std::string &file,
                            const std::string &delimiter) {
  StringFileHandler sfh(file);
  if (!sfh.HasNext()) {
    LOG_ERROR << "Empty file";
  }
  const auto header = sfh.GetLine();

  const auto columns = FlowString::splitNotEmpty(header, delimiter);
  std::ostringstream sql;
  sql << "COPY " << table << "(";

  for (size_t i = 0; i < columns.size(); ++i) {
    sql << columns.at(i);
    if (i != (columns.size() - 1)) {
      sql << ", ";
    }
  }

  sql << ") FROM STDIN CSV DELIMITER '" << delimiter << "';";

  auto connection = connections.get();
  // << HERE
  const auto res = PQexec(connection->object->connection, sql.str().c_str());

  if (isError(res, connection->object->connection) ||
      PQresultStatus(res) != PGRES_COPY_IN) {
    return;
  }

  int a = 1;

  while (sfh.HasNext() && a == 1) {
    const auto buffer = sfh.GetLine() + '\n';
    if (buffer.size() > 1) {
      a = PQputCopyData(connection->object->connection, buffer.c_str(),
                        buffer.size());
    }
  }
  const char *errmsg = nullptr;
  PQputCopyEnd(connection->object->connection, errmsg);

  const auto res_cp = PQgetResult(connection->object->connection);
  if (isError(res_cp, connection->object->connection)) {
    return;
  }

  PQclear(res);
  PQclear(res_cp);
  LOG_INFO << "Finished: " << sql.str();
}

std::unique_ptr<IdleObject<PgSqlDatabase_Connection>>
PgSqlDatabase::getConnection() {
  return connections.get();
}
