#pragma once

#include <string>
#include <mutex>
#include <map>


class FlowDatabaseDl {
public:
    static std::string dlwBlTo(const std::string &db, const std::string &path, const std::string &link);

    static void setProxy(const std::string& proxy);

private:
    static void lockDb(const std::string &name);

    static void unlockDb(const std::string &name);

    static std::map<std::string, std::mutex> _locks;

};
