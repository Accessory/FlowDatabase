#pragma once

#include <FlowUtils/FlowLog.h>
#include <map>
#include <memory>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <string>
#include <vector>

class RocksDbDatabase {
public:
  RocksDbDatabase(const std::string &path) {
    rocksdb::Options options;
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    std::vector<std::string> column_families_names;
    rocksdb::DB::ListColumnFamilies(options, path, &column_families_names);
    if (column_families_names.empty()) {
      rocksdb::DB *tmp;
      rocksdb::Status s = rocksdb::DB::Open(options, path, &tmp);
      db = std::unique_ptr<rocksdb::DB>(tmp);
      if (!s.ok()) {
        LOG_WARNING << "Could not establish RocksDB connection!";
        return;
      }

      db->Close();
      db = nullptr;
      rocksdb::DB::ListColumnFamilies(options, path, &column_families_names);
    }
    // else {
    std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyDescriptors;
    for (auto &name : column_families_names) {
      columnFamilyDescriptors.emplace_back(name,
                                           rocksdb::ColumnFamilyOptions());
    }

    std::vector<rocksdb::ColumnFamilyHandle *> handles;
    rocksdb::DB *tmp;
    rocksdb::Status s = rocksdb::DB::Open(
        options, path, columnFamilyDescriptors, &handles, &tmp);
    db = std::unique_ptr<rocksdb::DB>(tmp);
    if (!s.ok()) {
      LOG_WARNING << "Could not establish RocksDB connection!";
      return;
    }
    for (size_t i = 0; i < column_families_names.size(); ++i) {
      columnFamilyHandleMap.insert(
          make_pair(column_families_names.at(i), handles.at(i)));
    }
    // }
    isOpen = true;
    LOG_TRACE << "Opened DB: " << path;
  }

  ~RocksDbDatabase() {
    if (!db) {
      return;
    }

    rocksdb::CancelAllBackgroundWork(db.get(), true);

    for (auto columnFamily : columnFamilyHandleMap) {
      LOG_INFO << "Closeing Column Family Handle: " << columnFamily.first;
      // db->DropColumnFamily(columnFamily.second);
      db->DestroyColumnFamilyHandle(columnFamily.second);
    }
    // columnFamilyHandleMap.clear();

    db->Close();
    // db = nullptr;
    //        delete db;
  }

  bool IsOpen() { return isOpen; }

  void Put(const std::string &key, const std::string &value) {
    rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok())
      LOG_WARNING << "Failed to put value: " << value << "into key: " << key;
  }

  void DeleteKey(const std::string &key) {
    db->Delete(rocksdb::WriteOptions(), key);
  }

  void DeleteKey(const std::string &columnFamily, const std::string &key) {
    rocksdb::ColumnFamilyHandle *cfh = getColumnFamily(columnFamily);
    db->Delete(rocksdb::WriteOptions(), cfh, key);
  }

  void Put(const std::string &columnFamily, const std::string &key,
           const std::string &value) {
    rocksdb::ColumnFamilyHandle *cfh = getColumnFamily(columnFamily);
    rocksdb::Status s = db->Put(rocksdb::WriteOptions(), cfh, key, value);
    if (!s.ok())
      LOG_WARNING << "Failed to put value: " << value << "into key: " << key;
  }

  std::string Get(const std::string &key) {
    std::string value;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &value);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        LOG_TRACE << "Key not found.";
      } else {
        LOG_WARNING << "Failed to get value from key: " << key;
      }
    }
    return value;
  }

  std::string Get(const std::string &key, const std::string &columnFamily) {
    std::string value;
    rocksdb::ColumnFamilyHandle *cfh = getColumnFamily(columnFamily);

    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), cfh, key, &value);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        LOG_TRACE << "Key not found.";
      } else {
        LOG_WARNING << "Failed to get value from key: " << key;
      }
    }
    return value;
  }

  size_t GetSize() {
    size_t rtn = 0;
    std::unique_ptr<rocksdb::Iterator> itr(
        db->NewIterator(rocksdb::ReadOptions()));
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      ++rtn;
    }
    return rtn;
  }

  size_t GetSize(const std::string &columnFamily) {
    size_t rtn = 0;
    auto family = getColumnFamily(columnFamily);
    std::unique_ptr<rocksdb::Iterator> itr(
        db->NewIterator(rocksdb::ReadOptions(), family));
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      ++rtn;
    }
    return rtn;
  }

  std::vector<std::string> ListColumnFamilies() const {
    std::vector<std::string> rtn;
    for (const auto &[key, value] : columnFamilyHandleMap) {
      rtn.emplace_back(key);
    }
    return rtn;
  }

  std::vector<std::string> GetKeys() {
    std::vector<std::string> rtn;
    std::unique_ptr<rocksdb::Iterator> itr(
        db->NewIterator(rocksdb::ReadOptions()));
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      if (!itr->key().empty())
        rtn.emplace_back(itr->key().ToString());
    }
    return rtn;
  }
  std::vector<std::string> GetKeys(const std::string &columnFamily) {
    std::vector<std::string> rtn;
    rocksdb::ColumnFamilyHandle *cfh = getColumnFamily(columnFamily);
    std::unique_ptr<rocksdb::Iterator> itr(
        db->NewIterator(rocksdb::ReadOptions(), cfh));
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      if (!itr->key().empty())
        rtn.emplace_back(itr->key().ToString());
    }
    return rtn;
  }
  std::vector<std::string> GetAllValues() {
    std::vector<std::string> rtn;
    std::unique_ptr<rocksdb::Iterator> itr(
        db->NewIterator(rocksdb::ReadOptions()));
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      if (!itr->value().empty())
        rtn.emplace_back(itr->value().ToString());
    }
    return rtn;
  }

  std::vector<std::string> GetAllValues(const std::string &columnFamily) {
    std::vector<std::string> rtn;
    rocksdb::ColumnFamilyHandle *cfh = getColumnFamily(columnFamily);
    std::unique_ptr<rocksdb::Iterator> itr(
        db->NewIterator(rocksdb::ReadOptions(), cfh));
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      if (!itr->value().empty())
        rtn.emplace_back(itr->value().ToString());
    }
    return rtn;
  }

  void CreateColumnFamilyIfNotExists(const std::string &name) {
    getColumnFamily(name);
  }

  bool DeleteColumnFamily(const std::string &name) {
    std::lock_guard<std::mutex> guard(dbm);
    auto itr = columnFamilyHandleMap.find(name);
    if (itr == columnFamilyHandleMap.end()) {
      return false;
    }

    db->DropColumnFamily(itr->second);
    db->DestroyColumnFamilyHandle(itr->second);
    columnFamilyHandleMap.erase(itr);
    return true;
  }

private:
  rocksdb::ColumnFamilyHandle *
  getColumnFamily(const std::string &columnFamily) {
    std::lock_guard<std::mutex> guard(dbm);
    auto itr = columnFamilyHandleMap.find(columnFamily);
    if (itr != columnFamilyHandleMap.end()) {
      return itr->second;
    }

    rocksdb::ColumnFamilyHandle *handle;
    db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), columnFamily,
                           &handle);

    columnFamilyHandleMap.insert(make_pair(columnFamily, handle));

    return handle;
  }
  bool isOpen = false;

  std::mutex dbm;
  std::unique_ptr<rocksdb::DB> db;
  std::map<std::string, rocksdb::ColumnFamilyHandle *> columnFamilyHandleMap;
};
