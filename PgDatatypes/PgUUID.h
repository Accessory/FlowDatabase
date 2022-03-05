#pragma once

#include <string>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "PgColumn.h"

class PgUUID : public PgColumn<boost::uuids::uuid> {
public:
    PgUUID(std::string name) : PgColumn<boost::uuids::uuid>(std::move(name), "uuid") {}

    void Set(const char *data, size_t length) {
        boost::uuids::string_generator gen;
        this->_data = gen(data, data + length);
    };

    void SetValue(boost::uuids::uuid data) override {
        data.swap(_data);
    };

    boost::uuids::uuid GetValue() const override {
        return this->_data;
    };

    std::string ToString() const override {
        return boost::uuids::to_string(this->_data);
    };

private:
    boost::uuids::uuid _data;

};