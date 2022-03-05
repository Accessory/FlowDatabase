#pragma once

#include <string>
#include "PgColumn.h"
#include <charconv>

class PgBigInt : public PgColumn<int64_t> {
public:
    PgBigInt(std::string name) : PgColumn<int64_t>(std::move(name), "bitint") {}


    void Set(const char *chars, size_t length) {
        int64_t tmp;
        auto result = std::from_chars(chars, chars + length, tmp);
        if (result.ec == std::errc::invalid_argument) {
            return;
        }
        data = tmp;
    };

    int64_t GetValue() const override {
        return data;
    };

    std::string ToString() const override {
        return std::to_string(data);
    };

    void SetValue(int64_t value) override {
        data = std::move(value);
    }


private:
    int64_t data;

};