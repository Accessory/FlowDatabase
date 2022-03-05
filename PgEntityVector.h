#pragma once

#include "PgEntity.h"

class PgEntity;

class PgEntityVector : public std::vector<std::shared_ptr<PgColumnBase>> {
public:
    virtual PgEntity New() const = 0;
};