//
// Created by matt on 5/3/20.
//

#include "normal/pushdown/AWSClient.h"

#include <aws/core/Aws.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <normal/plan/Globals.h>
#include <normal/pushdown/Globals.h>

namespace normal::pushdown {

void AWSClient::init() {
  options_.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
  Aws::InitAPI(options_);
}

  [[maybe_unused]] void AWSClient::shutdown() {
  Aws::ShutdownAPI(options_);
}

std::shared_ptr<Aws::S3::S3Client> AWSClient::defaultS3Client() {
  if (!initialized_) {
    auto client = std::make_shared<AWSClient>();
    client->init();
    initialized_ = true;
  }

  static const char *ALLOCATION_TAG = "Normal";

  std::shared_ptr<Aws::S3::S3Client> s3Client;

  Aws::Client::ClientConfiguration config;
  config.region = Aws::Region::US_WEST_1;
  config.scheme = Aws::Http::Scheme::HTTP;
  // This value has been tuned for c5a.4xlarge, c5a.8xlarge, and c5n.9xlarge, any more connections than this and aggregate
  // network performance degrades rather than remaining constant
  // This value makes low selectivity S3 Select requests much faster as it utilizes more network bandwidth for them.
  // S3 Select requests with high selectivity are unaffected, as are GET requests (so for GET we don't run it in parallel)
  config.maxConnections = 200; // Default = 25
  config.retryStrategy = Aws::MakeShared<Aws::Client::DefaultRetryStrategy>(ALLOCATION_TAG, 0, 0); // Disable retries
  config.connectTimeoutMs = 500000;
  config.requestTimeoutMs = 900000;
  // Default is to create and dispatch to thread with async methods, we don't use async so default is ideal
  config.executor = Aws::MakeShared<Aws::Utils::Threading::DefaultExecutor>(ALLOCATION_TAG);
  config.verifySSL = false;

  // Commented this out as turning it off increase transfer rate. It appears that having this set
  // reduces transfer rate even if the chosen value is very high.
  if (normal::pushdown::NetworkLimit > 0) {
    std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> limiter;
    limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(ALLOCATION_TAG, normal::pushdown::NetworkLimit);
    config.readRateLimiter = limiter;
    config.writeRateLimiter = limiter;
  }

  switch (normal::plan::s3ClientType) {
    case normal::plan::S3: {
      SPDLOG_INFO("Using S3 Client");
      s3Client = Aws::MakeShared<Aws::S3::S3Client>(
              ALLOCATION_TAG,
              Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>(ALLOCATION_TAG),
              config,
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              true);
      break;
    }
    case normal::plan::Airmettle: {
      SPDLOG_INFO("Using Airmettle Client");
      config.endpointOverride = "54.151.121.20/s3/test";
      Aws::String accessKeyId = "test-test";
      Aws::String secretKey = "test";
      Aws::Auth::AWSCredentials airmettleCredentials = Aws::Auth::AWSCredentials(accessKeyId, secretKey);

      s3Client = Aws::MakeShared<Aws::S3::S3Client>(
              ALLOCATION_TAG,
              airmettleCredentials,
              config,
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              false);
      break;
    }
    case normal::plan::Minio: {
      SPDLOG_INFO("Using Minio Client");
      config.endpointOverride = "172.31.10.231:9000";
      Aws::String accessKeyId = "minioadmin";
      Aws::String secretKey = "minioadmin";
      Aws::Auth::AWSCredentials minioCredentials = Aws::Auth::AWSCredentials(accessKeyId, secretKey);

      s3Client = Aws::MakeShared<Aws::S3::S3Client>(
              ALLOCATION_TAG,
              minioCredentials,
              config,
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              false);
      break;
    }
    case normal::plan::Ceph: {
      SPDLOG_INFO("Using Ceph Client");
      config.endpointOverride = "192.168.6.95:8000";
      std::ifstream configFile("/root/.s3cfg");
      if (!configFile.is_open()) {
        throw std::runtime_error("Error: Unable to open .s3cfg file.");
      }
      std::string line;
      std::regex accessKeyRegex(R"(access_key = (.+))");
      std::regex secretKeyRegex(R"(secret_key = (.+))");

      std::smatch match;
      Aws::String accessKeyId;
      Aws::String secretKey;

      while (std::getline(configFile, line)) {
        if (std::regex_search(line, match, accessKeyRegex)) {
            accessKeyId = match[1].str();;
        } else if (std::regex_search(line, match, secretKeyRegex)) {
            secretKey = match[1].str();
        }
      }

      configFile.close();
      // Check if both Access Key and Secret Key are found
      if (accessKeyId.empty() || secretKey.empty()) {
        throw std::runtime_error("Error: Access Key or Secret Key not found in .s3cfg file.");
      }
      Aws::Auth::AWSCredentials cephCredentials = Aws::Auth::AWSCredentials(accessKeyId, secretKey);

      s3Client = Aws::MakeShared<Aws::S3::S3Client>(
              ALLOCATION_TAG,
              cephCredentials,
              config,
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              false);
      break;
    }
    default: {
      throw std::runtime_error("Bad S3Client Type");
    }
  }

  return s3Client;
}

}
