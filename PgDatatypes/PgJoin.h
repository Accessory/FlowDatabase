#pragma once

#include <string>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <FlowDatabase/PgEntity.h>
#include "PgColumn.h"
#include "../PgEntity.h"
//#include "/home/hermes/Syncthing/Incubation/MeMa/entities/Tag_Entity.h"

class PgJoin : public PgColumn<std::vector<std::shared_ptr<PgEntity>>> {
public:
    PgJoin(std::string name, std::shared_ptr<PgEntity> entity, std::string on, std::string onTo) :
            On(on),
            OnTo(onTo),
            ref(entity),
            PgColumn<std::vector<std::shared_ptr<PgEntity>>>(std::move(name), "ref") {
        IsRef = true;
    }

    void Set(const char *data, const size_t length) {
        if (ref == nullptr) { return; }
        std::string_view view(data, length);
        std::vector<std::shared_ptr<PgEntity>> new_data;

        if (!view.starts_with("{\"")) {
            return;
        }
        size_t pos = 2;
        while (pos < length && view.at(pos) == '(') {
            auto item = std::shared_ptr<PgEntity>(ref->NewPtr());
            const auto column_size = item->size();
            for (size_t i = 0; i < column_size; ++i) {
                if (item->at(i)->IsRef) {
                    continue;
                }

                ++pos;
                if (pos >= length)
                    return;
                size_t end = 0;
                size_t next = 0;
                if (view.at(pos) == '\\' && view.at(pos + 1) == '"') {
                    pos += 2;
                    end = view.find_first_of("\\", pos);
                    while (end < length - 1 && view.at(end + 1) != '"') {
                        end = view.find_first_of("\\", end + 2);
                    }

                    next = end + 2;
                } else {
                    end = view.find_first_of(",)", pos);
                    next = end;
                }
                item->at(i)->Get<PgColumnBase>().Set(&data[pos], end - pos);
                pos = next;
            }
            new_data.emplace_back(item);

            if (pos < length - 2 && view.at(pos) == ')' && view.at(pos + 1) == '"' && view.at(pos + 2) == '}') {
                break;
            }

            if (pos < length - 3 && view.at(pos) == ')' && view.at(pos + 1) == '"' && view.at(pos + 2) == ',' &&
                view.at(pos + 3) == '"') {
                pos += 4;
            }

        }
        _data.swap(new_data);
    }


    void Set2(const char *data, size_t length) {
//        LOG_DEBUG << std::string(data, length);
        if (ref == nullptr) { return; }


        std::vector<std::shared_ptr<PgEntity>> new_data;
        size_t pos = 0;

        if (data[pos] != '{' || data[++pos] != '"') {
            return;
        }
        while (++pos < length && data[pos] == '(') {
            auto item = std::make_shared<PgEntity>(ref->New());
            const auto column_size = item->size();
            for (size_t i = 0; i < column_size; ++i) {
                if (item->at(i)->IsRef) {
                    continue;
                }

                ++pos;
                if (pos >= length)
                    return;
                const auto start = pos;

                while (pos < length && data[pos] != ',' && data[pos] != ')') {
//                    if(pos == '\\'){
//                        ++pos;
//                        if(pos >= length)
//                            return;
//                    }
                    ++pos;
                }
                const auto end = pos;
                item->at(i)->Get<PgColumnBase>().Set(&data[start], end - start);
            }
            new_data.emplace_back(item);

            if (++pos < length && data[pos] == '"') {
                if (++pos < length && data[pos] == ',') {
                    if (++pos < length && data[pos] == '"') {
                        continue;
                    }
                }
                if (pos == '}') {
                    break;
                }
            }
        }

        _data.swap(new_data);
    }

    void SetValue(std::vector<std::shared_ptr<PgEntity>> data) override {

    }

    std::vector<std::shared_ptr<PgEntity>> GetValue() const override {
        return this->_data;
    }

    std::vector<std::shared_ptr<PgEntity>> *GetData() {
        return &this->_data;
    }


    std::string ToString() const override {
        std::ostringstream rtn;
        rtn << "[";
        for (size_t i = 0; i < _data.size(); ++i) {
            if (i != 0)
                rtn << ',';
            rtn << "{";
            for (size_t y = 0; y < _data.at(i)->size(); ++y) {
                if (y != 0)
                    rtn << ", ";
                rtn << '"' << _data.at(i)->at(y)->Name << '"';
                rtn << " : ";
                rtn << '"' << _data.at(i)->at(y)->ToString() << '"';
            }
            rtn << "}";
        }
        rtn << "]";
        return rtn.str();
    };

    std::string On;
    std::string OnTo;
private:
    std::shared_ptr<PgEntity> ref;
    std::vector<std::shared_ptr<PgEntity>> _data;

};