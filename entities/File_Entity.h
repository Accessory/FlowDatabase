#pragma once

#include <FlowDatabase/PgEntity.h>
#include <FlowDatabase/PgDatatypes/PgVarChar.h>
#include <FlowDatabase/PgDatatypes/PgUUID.h>
#include <FlowDatabase/PgDatatypes/PgBigInt.h>
#include <FlowDatabase/PgDatatypes/PgJson.h>
#include <FlowDatabase/PgDatatypes/PgDateTime.h>
#include <memory>
#include "Tag_Entity.h"
#include "../PgDatatypes/PgJoin.h"

class File_Entity : public PgEntity {
public:
    File_Entity() : PgEntity("file") {
        this->emplace_back(id);
        PrimaryKey = id;
        this->emplace_back(path);
        this->emplace_back(name);
        this->emplace_back(type);
        this->emplace_back(meta);
        this->emplace_back(size);
        this->emplace_back(created);
        this->emplace_back(nr);
        this->emplace_back(tag);
    }

    std::shared_ptr<PgUUID> id = std::make_shared<PgUUID>("id");
    std::shared_ptr<PgVarChar> path = std::make_shared<PgVarChar>("path");
    std::shared_ptr<PgVarChar> name = std::make_shared<PgVarChar>("name");
    std::shared_ptr<PgVarChar> type = std::make_shared<PgVarChar>("type");
    std::shared_ptr<PgBigInt> size = std::make_shared<PgBigInt>("size");
    std::shared_ptr<PgJson> meta = std::make_shared<PgJson>("meta");
    std::shared_ptr<PgDateTime> created = std::make_shared<PgDateTime>("created");
    std::shared_ptr<PgBigInt> nr = std::make_shared<PgBigInt>("nr");
    std::shared_ptr<PgJoin> tag = std::make_shared<PgJoin>("tag", std::make_shared<Tag_Entity>(), "id", "file_id");

    [[nodiscard]] PgEntity New() const override {
        return File_Entity();
    }

};