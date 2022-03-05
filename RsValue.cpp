#include "RsValue.h"
#include <cstring>

RsValue::RsValue(char *data) {
    if (data == nullptr)
        return;
    size_t len = std::strlen(data);
    this->insert(this->begin(), data, data + len);
}

std::string RsValue::toString() const {
    return std::string(this->begin(), this->end());
}

int RsValue::toInteger() const {
    return (int) std::strtol(toString().data(), nullptr, 10);
}

long RsValue::toLong() const {
    return std::strtol(toString().data(), nullptr, 10);
}

bool RsValue::toBoolean() const {
    return std::strtol(reinterpret_cast<const char *>(this->data()), nullptr, 10) == 1;
}

RsValue::RsValue(char *data, size_t len) {
    this->insert(this->begin(), data, data + len);
}

RsValue::RsValue(const unsigned char *data, size_t len) {
    this->insert(this->begin(), data, data + len);
}
