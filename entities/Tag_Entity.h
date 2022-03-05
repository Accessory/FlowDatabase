#pragma once

#include <FlowDatabase/PgEntity.h>
#include <FlowDatabase/PgDatatypes/PgVarChar.h>
#include <FlowDatabase/PgDatatypes/PgUUID.h>
#include <FlowDatabase/PgDatatypes/PgBigInt.h>
#include <FlowDatabase/PgDatatypes/PgJson.h>
#include <FlowDatabase/PgDatatypes/PgDateTime.h>
#include <memory>

class Tag_Entity : public PgEntity {
public:
    Tag_Entity() : PgEntity("tag") {
        this->emplace_back(id);
        PrimaryKey = id;
        this->emplace_back(name);
        this->emplace_back(file_id);
    }

    std::shared_ptr<PgUUID> id = std::make_shared<PgUUID>("id");
    std::shared_ptr<PgVarChar> name = std::make_shared<PgVarChar>("name");
    std::shared_ptr<PgUUID> file_id = std::make_shared<PgUUID>("file_id");

    [[nodiscard]] PgEntity New() const override {
        return Tag_Entity();
    }

};