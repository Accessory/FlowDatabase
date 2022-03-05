#include <FlowHtmlGod.h>
#include "FlowDatabaseDl.h"
#include "FlowOpenSSL.h"
#include <FlowUtils/FlowFile.h>
#include "FlowRDBGod.h"

std::map<std::string, std::mutex> FlowDatabaseDl::_locks;

std::string FlowDatabaseDl::dlwBlTo(const std::string &db, const std::string &path, const std::string &link) {

    LOG_DEBUG << "Start with: " << link;

    std::string filename = link.substr(link.find_last_of("/") + 1);
    boost::filesystem::path file(path);
    file /= filename;


    FlowFile::createDirIfNotExist(path, false);
    if (FlowFile::fileExist(file.string())) {
        LOG_DEBUG << "File exist.";
        return "";
    }
    lockDb(db);
    std::string dbValue = FlowRDBGod::getKey(db, filename, "files");
    if (!dbValue.empty()) {
        LOG_DEBUG << "File was already downloaded once.";
        unlockDb(db);
        return "";
    }
    FlowRDBGod::writeToRDB(db, filename, "True", "files");
    unlockDb(db);

    auto data = FlowHtmlGod::download(link);
    if(data.empty()){
        LOG_WARNING << "Failed Download.";
        return "";
    }
    std::string sha = FlowOpenSSL::generateSha256(data);
    dbValue = FlowRDBGod::getKey(db, sha, "hash");
    FlowRDBGod::writeToRDB(db, sha, filename, "hash");
    if (!dbValue.empty()) {
        LOG_DEBUG << "Hash was found.";
        return "";
    }

    FlowFile::writeBinaryVector(file.string(), data);


    return file.string();
}

void FlowDatabaseDl::setProxy(const std::string &proxy) {
    FlowHtmlGod::proxy = proxy;
}

void FlowDatabaseDl::lockDb(const std::string &name) {
    _locks[name].lock();
}

void FlowDatabaseDl::unlockDb(const std::string &name) {
    _locks[name].unlock();
}

