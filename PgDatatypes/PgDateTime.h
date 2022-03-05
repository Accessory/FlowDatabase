#pragma once

#include "PgColumn.h"
#include <string>
#include <sstream>

#define dateParserString "%Y-%m-%d %H:%M:%S"

class PgDateTime : public PgColumn<std::tm> {
public:
    explicit PgDateTime(std::string name) : PgColumn<std::tm>(std::move(name), "timestamp without time zone") {}


    void Set(const char *chars, size_t length) override {
        std::string buffer(chars, length);
        std::istringstream in(buffer);
        in >> std::get_time(&data, dateParserString);
    };

    [[nodiscard]] std::tm GetValue() const override {
        return data;
    };

    [[nodiscard]] std::string ToString() const override {
        std::stringstream ss;
        ss << std::put_time(&data, dateParserString);
        return ss.str();
    };

    void SetValue(std::tm value) override {
        data = value;
    }


private:
    std::tm data;

};