
#include <unistd.h>

#include <iostream>
#include <iomanip>
#include <fstream>
#include <vector>
#include <string>

#include <boost/filesystem.hpp>

#include <cryptopp/md5.h>
#include <cryptopp/hex.h>
#include <cryptopp/files.h>

#include "bucket.hpp"

using namespace std;

namespace s3exp {
   struct Configuration {
      string exportPath;
      string bucketExportPath;
      string s3AccessKey;
      string s3SecretKey;
      string s3Endpoint;
      string s3Bucket;
      bool shouldSsl = false;
      bool isValid = false;
      bool listOnly = false;
      int exportMaxCount = 0;
   };

   void printHelp() {
      cout << endl;
      cout << "S3 Exporter" << endl;
      cout << "mailto: clas@gmx.net" << endl;
      cout << "" << endl;
      cout << "  Arguments: " << endl;
      cout << "     exp=<ExportPath>" << endl;
      cout << "     s3e=<S3Endpoint>" << endl;
      cout << "     s3b=<S3Bucket>" << endl;
      cout << "     s3a=<S3AccessKey>" << endl;
      cout << "     s3s=<S3SecretKey>" << endl;
      cout << "     ssl=<true|false>" << endl;
      cout << "     exm=<MaxExport>" << endl;
      cout << "     exl=<true|false>" << endl;
      cout << endl;
   }

   void logConfig(Configuration& config) {
      cout << "::" << endl;

      auto getBoolStr = [] (bool value) {
                           return value ? "True" : "False";
                        };
      
      cout << "::  ExportPath    : " << config.exportPath << endl;
      cout << "::  S3EndPoint    : " << config.s3Endpoint << endl;
      cout << "::  S3Bucket      : " << config.s3Bucket << endl;
      cout << "::  S3AccessKey   : " << config.s3AccessKey << endl;
      cout << "::  S3SecretKey   : " << config.s3SecretKey << endl;
      cout << "::  SSLOn         : " << getBoolStr(config.shouldSsl) << endl;
      cout << "::  BucketExportP : " << config.bucketExportPath << endl;
      cout << "::  MaxKeys       : " << config.exportMaxCount << endl;
      cout << "::  ListOnly      : " << getBoolStr(config.listOnly) << endl;
      cout << "::" << endl;
   }

   void validateConfig(Configuration& config) {
      config.bucketExportPath = "";
      config.bucketExportPath += config.exportPath;
      if (config.bucketExportPath[config.bucketExportPath.size()-1] != '/') {
         config.bucketExportPath += "/";
      }
      config.bucketExportPath += config.s3Bucket;

      boost::filesystem::create_directories(config.bucketExportPath);
      
      config.isValid = boost::filesystem::is_empty(config.bucketExportPath);

      if (!config.isValid)
         cout << "::Err " << config.bucketExportPath << " is not empty!" << endl;
   }

   Configuration readConfig(int argc, char *argv[]) {
      Configuration result;
      for (auto i = 1; i < argc; i++) {
         string configId;
         configId += argv[i][0];
         configId += argv[i][1];
         configId += argv[i][2];
         string configValue = (argv[i])+4;
         if (!(string("exp")).compare(configId)) {
            result.exportPath = configValue;
            continue;
         }
         if (!(string("s3e")).compare(configId)) {
            result.s3Endpoint = configValue;
            continue;
         }
         if (!(string("s3b")).compare(configId)) {
            result.s3Bucket = configValue;
            continue;
         }
         if (!(string("s3a")).compare(configId)) {
            result.s3AccessKey = configValue;
            continue;
         }
         if (!(string("s3s")).compare(configId)) {
            result.s3SecretKey = configValue;
            continue;
         }
         if (!(string("ssl")).compare(configId)) {
            result.shouldSsl = configValue=="true" ? true : false;
            continue;
         }
         if (!(string("exm")).compare(configId)) {
            result.exportMaxCount  = atoi(configValue.c_str());
            continue;
         }
         if (!(string("exl")).compare(configId)) {
            result.listOnly = configValue=="true" ? true : false;
            continue;
         }
         
      }
      validateConfig(result);
      return result;
   }

   string createMd5HashOfFile(string& exportedFile) {
      std::string result;
      CryptoPP::Weak::MD5 hash;
      CryptoPP::FileSource(exportedFile.c_str(),true,new
                           CryptoPP::HashFilter(hash,new CryptoPP::HexEncoder(new
                                                                              CryptoPP::StringSink(result),false)));
      return result;
   }

   Bucket::BucketItemsList exportBucket(const Configuration& config) {
      S3_initialize("s3", S3_INIT_ALL, config.s3Endpoint.c_str());
      cout << "::Begin export bucket \"" << config.s3Bucket.c_str() << "\" to \"" << config.exportPath.c_str() << "\"" << endl;
      cout << "::" << endl;
        
      S3BucketContext bc;
      bc.hostName = config.s3Endpoint.c_str();
      bc.bucketName = config.s3Bucket.c_str();
      bc.protocol = config.shouldSsl ? S3ProtocolHTTPS : S3ProtocolHTTP;
      bc.uriStyle = S3UriStylePath;
      bc.accessKeyId = config.s3AccessKey.c_str();
      bc.secretAccessKey = config.s3SecretKey.c_str();
      bc.securityToken  = nullptr;
      bc.authRegion = nullptr;
              
      Bucket bucket(bc);

      auto action = [config,bucket] (const Bucket::BucketItemData& bucketItem) {
                       cout << ":: Bg MD5[" << bucketItem.etag << "] "; 
                       cout << "SIZE[" << setfill ('0') << std::setw (20) << bucketItem.size << "] ";
                       cout << "KEY[" << bucketItem.key << "]" << endl;
                       if (!config.listOnly) {
                          auto exportedItemFileName = bucket.exportItem(bucketItem, config.bucketExportPath);
                          auto calcedHash = createMd5HashOfFile(exportedItemFileName);
                          string realCalcedHash = calcedHash;
                          if (bucketItem.etag[0] == '"') {
                             realCalcedHash =  "\"";
                             realCalcedHash += calcedHash;
                             realCalcedHash += "\"";
                          }
                          if (!realCalcedHash.compare(bucketItem.etag)) {
                             cout << ":: Fn Succeeded : Computed Hash : " << calcedHash.c_str() << endl;
                          }
                          else {
                             cout << ":: Fn Failed    : Computed Hash : " << calcedHash.c_str() << endl;
                             exit(100);
                          }
                       }
                    };
      
      Bucket::BucketItemsList bucketItems =
         bucket.getItems(config.exportMaxCount, action);

      S3_deinitialize();

      return bucketItems;
   }
}

int main(int argc, char *argv[]) {
   try {
      if (argc < 7) {
         s3exp::printHelp();
         return 0;
      }
      
      cout << "::" << endl;
      cout << "::S3 Exporter Start" << endl;
      cout << "::" << endl;
      cout << "::Read Configuration" << endl;
      
      s3exp::Configuration config = s3exp::readConfig(argc, argv);
      if (config.isValid) {
         cout << "::Ok" << endl;
         logConfig(config);
         auto result = s3exp::exportBucket(config);
         cout << "::" << endl;
         cout << ":: ExportedCount : " << setfill ('0') << std::setw (30) << result.count << endl;
         cout << ":: ExportedSize  : " << setfill ('0') << std::setw (30) << result.size << endl;
         cout << "::" << endl;
         cout << "::S3 Exporter Finnish" << endl;
         cout << "::" << endl;
         return 0;
      }

      cout << "::Err -> Invalid Configuration" << endl;
      return 1;
   }
   catch (...) {
      return 2;
   }
}

