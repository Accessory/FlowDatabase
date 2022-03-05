#pragma once

#include "PgColumn.h"
#include <string>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <rapidjson/prettywriter.h>

class PgJson : public PgColumn<rapidjson::Document> {
public:
    explicit PgJson(std::string name) : PgColumn<rapidjson::Document>(std::move(name), "jsonb") {}


    void Set(const char *chars, size_t length) override {
        rapidjson::Document document;
        document.Parse(chars, length);
        if (document.HasParseError()) {
            return;
        }
        data = std::move(document);
    };

    [[nodiscard]] rapidjson::Document GetValue() const override {
        rapidjson::Document document;
        document.CopyFrom(data, document.GetAllocator());
        return document;
    };

    [[nodiscard]] std::string ToString() const override {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        data.Accept(writer);
        return std::string(buffer.GetString());
    };

    [[nodiscard]] std::string ToPrettyString() const {
        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        data.Accept(writer);
        return std::string(buffer.GetString());
    };

    void SetValue(rapidjson::Document value) override {
        data = std::move(value);
    }


private:
    rapidjson::Document data;

};