#pragma once

#include <string>

class PgColumnBase {
public:
    PgColumnBase(std::string name, std::string type) : Name(std::move(name)), Type(std::move(type)) {}

    virtual ~PgColumnBase() = default;;
    std::string Name;
    std::string Type;
    bool Dirty = true;
    bool IsPrimary = false;
    bool IsRef = false;


    virtual void Set(const char *chars, size_t length)  = 0;

    [[nodiscard]] virtual std::string ToString() const = 0;

    template<class T>
    T &Get() {
        return reinterpret_cast<T &>(*this);
    };
};