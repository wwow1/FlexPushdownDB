//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_OPERATORS_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_OPERATORS_H

#include <vector>
#include <memory>

#include <normal/pushdown/AWSClient.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/filter/Filter.h>
#include <normal/pushdown/Collate.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/pushdown/shuffle/Shuffle.h>

using namespace normal::pushdown;
using namespace normal::pushdown::filter;
using namespace normal::pushdown::join;
using namespace normal::pushdown::shuffle;

namespace normal::ssb::query1_1 {

/**
 * Normal operator factories for SSB query 1.1
 */
class Operators {

public:

  static std::vector<std::shared_ptr<FileScan>>
  makeDateFileScanOperators(const std::string &dataDir, int numConcurrentUnits);

  static std::vector<std::shared_ptr<S3SelectScan>>
  makeDateS3SelectScanOperators(const std::string &s3ObjectDir,
								const std::string &s3Bucket,
								int numConcurrentUnits,
								std::unordered_map<std::string, long> partitionMap,
								AWSClient &client);

  static std::vector<std::shared_ptr<S3SelectScan>>
  makeDateS3SelectScanPushDownOperators(const std::string &s3ObjectDir,
										const std::string &s3Bucket,
										short year,
										int numConcurrentUnits,
										std::unordered_map<std::string, long> partitionMap,
										AWSClient &client);

  static std::vector<std::shared_ptr<FileScan>>
  makeLineOrderFileScanOperators(const std::string &dataDir, int numConcurrentUnits);

  static std::vector<std::shared_ptr<S3SelectScan>>
  makeLineOrderS3SelectScanOperators(const std::string &s3ObjectDir,
									 const std::string &s3Bucket,
									 int numConcurrentUnits,
									 std::unordered_map<std::string, long> partitionMap,
									 AWSClient &client);

  static std::vector<std::shared_ptr<S3SelectScan>>
  makeLineOrderS3SelectScanPushdownOperators(const std::string &s3ObjectDir,
											 const std::string &s3Bucket,
											 short discount, short quantity,
											 int numConcurrentUnits,
											 std::unordered_map<std::string, long> partitionMap,
											 AWSClient &client);

  static std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
  makeDateFilterOperators(short year, int numConcurrentUnits);

  static std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
  makeLineOrderFilterOperators(short discount, short quantity, int numConcurrentUnits);

  static std::vector<std::shared_ptr<Shuffle>>
  makeDateShuffleOperators(int numConcurrentUnits);

  static std::vector<std::shared_ptr<Shuffle>>
  makeLineOrderShuffleOperators(int numConcurrentUnits);

  static std::vector<std::shared_ptr<HashJoinBuild>>
  makeHashJoinBuildOperators(int numConcurrentUnits);

  static std::vector<std::shared_ptr<HashJoinProbe>>
  makeHashJoinProbeOperators(int numConcurrentUnits);

  static std::vector<std::shared_ptr<Aggregate>>
  makeAggregateOperators(int numConcurrentUnits);

  static std::shared_ptr<Aggregate> makeAggregateReduceOperator();

  static std::shared_ptr<Collate> makeCollateOperator();

};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_OPERATORS_H