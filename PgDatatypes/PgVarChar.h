#pragma once

#include <string>
#include "PgColumn.h"

class PgVarChar : public PgColumn<std::string> {
public:
    explicit PgVarChar(std::string name,
              size_t max_length = 255) : max_length(max_length),
                                         PgColumn<std::string>(std::move(name), "character varying") {}


    void Set(const char *chars, size_t length) override {
        this->data.assign(chars, chars + length);
    };

    [[nodiscard]] std::string GetValue() const override {
        return data;
    };

    [[nodiscard]] std::string ToString() const override {
        return GetValue();
    };

    void SetValue(std::string value) override {
        data = std::move(value);
    }


private:
    std::string data;
    size_t max_length;

};