//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H

#include <memory>
#include <string>

#include <normal/sql/connector/CatalogueEntry.h>
#include <normal/sql/logical/ScanLogicalOperator.h>
#include <normal/sql/connector/Catalogue.h>

namespace normal::sql::connector::local_fs {

class LocalFileSystemCatalogueEntry: public normal::sql::connector::CatalogueEntry {

private:
  std::string path_;

public:
  LocalFileSystemCatalogueEntry(const std::string &Alias, std::string Path, std::shared_ptr<Catalogue>);
  ~LocalFileSystemCatalogueEntry() override = default;

  [[nodiscard]] const std::string &getPath() const;

  std::shared_ptr<logical::ScanLogicalOperator> toLogicalOperator() override ;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H