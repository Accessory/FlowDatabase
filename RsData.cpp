#include "RsData.h"
#include <FlowUtils/FlowLog.h>
#include <algorithm>
#include <iomanip>
#include <sstream>

std::string RsData::printData() const {
  std::stringstream rtn;

  std::vector<size_t> lenghts;
  size_t row = 0;
  for (auto &pair : *this) {
    size_t cMaxLength =
        max_element(pair.second.begin(), pair.second.end(), lengthCompare)
            ->size();
    cMaxLength = std::max(cMaxLength, pair.first.length());
    lenghts.emplace_back(cMaxLength + 2);
    row = std::max(pair.second.size(), row);
    rtn << std::setw(static_cast<int>(lenghts.back())) << std::left
        << pair.first;
  }
  rtn << std::endl;
  for (size_t i = 0; i < row; ++i) {
    if (i > 0)
      rtn << std::endl;
    size_t pos = 0;
    for (auto &pair : *this) {
      rtn << std::setw(static_cast<int>(lenghts.at(pos++))) << std::left
          << pair.second.at(i).toString();
    }
  }
  auto var = rtn.str();
  //   var.length();
  return rtn.str();
}

size_t RsData::rowSize() const {
  if (this->size() == 0)
    return 0;
  return max_element((*this).begin(), (*this).end(), RsData::sizeCompare)
      ->second.size();
}

bool RsData::isEmpty() const {
  if (this->empty())
    return true;
  for (const auto &item : *this) {
    if (!item.second.empty())
      return false;
  }
  return true;
}

std::vector<std::string> RsData::toStringVector(const std::string &key) const {
  const std::vector<RsValue> &val = this->at(key);
  std::vector<std::string> rtn;
  rtn.reserve(val.size());
  for (const RsValue &v : val) {
    rtn.emplace_back(v.toString());
  }
  return rtn;
}

std::vector<std::string> RsData::getColumnNames() const {
  std::vector<std::string> rtn;
  for (const auto &item : (*this)) {
    rtn.emplace_back(item.first);
  }
  return rtn;
}
