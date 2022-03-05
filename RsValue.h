#pragma once

#include <vector>
#include <string>

class RsValue : public std::vector<unsigned char>{
public:
    RsValue(char *  data);
    RsValue(char *data, size_t len);
    RsValue(const unsigned char *data, size_t len);
    std::string toString() const;
    int toInteger() const;
    long toLong() const;
    bool toBoolean() const;
};