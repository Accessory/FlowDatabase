#pragma once

#include <string>
#include <map>
#include <vector>
#include "RsValue.h"

class RsData : public std::map<std::string, std::vector<RsValue>> {
public:
    std::string printData() const;

    static bool lengthCompare(const RsValue &l, const RsValue &r) { return l.size() < r.size(); }

    static bool sizeCompare(const std::pair<std::string, std::vector<RsValue>> &l,
                            const std::pair<std::string, std::vector<RsValue>> &r) {
        return l.second.size() < r.second.size();
    }

    size_t rowSize() const;

    std::vector<std::string> toStringVector(const std::string &key) const;

    std::vector<std::string> getColumnNames() const;


};


