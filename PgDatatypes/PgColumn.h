#pragma once

#include <string>
#include "PgColumnBase.h"

template<class T>
class PgColumn : public PgColumnBase {
public:
    PgColumn(std::string name, std::string type) : PgColumnBase(std::move(name), std::move(type)) {}

    virtual T GetValue() const = 0;

    virtual void SetValue(T rhs) = 0;
};