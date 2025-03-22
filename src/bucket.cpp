
#include "bucket.hpp"

#include <iostream>
#include <boost/filesystem.hpp>

namespace s3exp {
   const int ErrCustomMaxKeyCountReached = -100;
   
   void Bucket::completeCallback(S3Status status, const S3ErrorDetails *error, void *cbData)
   {
      callbackData *data = static_cast<callbackData *>(cbData);
      data->s3Status = status;
      return;
   }
   
   S3Status Bucket::dataCallback(int bufferSize, const char *buffer, void *cbData)
   {
      callbackData *data = static_cast<callbackData *>(cbData);
      size_t wrote = fwrite(buffer, 1, bufferSize, data->FileHandle);
      return ((wrote < (size_t) bufferSize) ? S3StatusAbortedByCallback : S3StatusOK);
   }

   S3Status Bucket::propertiesCallback(const S3ResponseProperties *properties, void *callbackData)
   {
      return S3StatusOK;
   }
   
   S3Status Bucket::listCallback(int isTruncated,
                               const char *nextMarker,
                               int contentsCount,
                               const S3ListBucketContent *contents,
                               int commonPrefixesCount,
                               const char **commonPrefixes,
                               void *cbData) {

      callbackData *data = static_cast<callbackData *>(cbData);

      data->isTruncated = isTruncated;
      
      if ((!nextMarker || !nextMarker[0]) && contentsCount) {
         nextMarker = contents[contentsCount - 1].key;
      }
      if (nextMarker) {
         data->nextMarker = nextMarker;
      }
      else {
         data->nextMarker = "";
      }

      for (int i = 0; i < contentsCount; i++) {
         if (data->maxKeyCount && (data->items->count > data->maxKeyCount-1)) {
            data->maxKeyCountIsReached = true;
            return S3StatusInterrupted;
         }
        
         const S3ListBucketContent *content = &(contents[i]);

         Bucket::BucketItemData bucketData;
         bucketData.key = content->key;
         bucketData.etag = content->eTag;
         bucketData.size = content->size;

         data->items->count++;
         data->items->size += content->size;

         if (data->action) {
            data->action(bucketData);
         }
         else {
            data->items->items.push_back(bucketData);
         }
      }

      return S3StatusOK;
   }
   
   Bucket::BucketItemsList Bucket::getItems(int maxKeyCount,
                                            function<void(const BucketItemData&)> action) const {
      BucketItemsList result;
      
      S3ResponseHandler responseHandler = {
         &propertiesCallback,
         &completeCallback
      };

      S3ListBucketHandler listBucketHandler =  {
         responseHandler,
         &listCallback
      };
      
      callbackData cb;
      
      cb.bucketContext = _bucketcontext;
      cb.items = &result;
      cb.action = action;
      cb.maxKeyCount = maxKeyCount;

      do {
         cb.isTruncated = 0;

         S3_list_bucket(&_bucketcontext,
                        nullptr,
                        cb.nextMarker != "" ? cb.nextMarker.c_str() : nullptr,
                        nullptr,
                        0,
                        nullptr,
                        0,
                        &listBucketHandler,
                        &cb);
         
      } while ((cb.s3Status == S3StatusOK) && cb.isTruncated && (cb.maxKeyCountIsReached==false));

      return result;
   }

   string Bucket::createExportFilePath(const Bucket::BucketItemData& bucketItem, string& exportPath) const {
      string outPath = exportPath;
      if (outPath[outPath.size()-1] != '/')
         outPath += "/";
      
      if (bucketItem.key.find_first_of("/")  == string::npos) {
         outPath += bucketItem.key;
      }
      else {
         outPath += bucketItem.key.substr(0, bucketItem.key.find_last_of("/"));
         boost::filesystem::create_directories(outPath);
         outPath += bucketItem.key.substr(bucketItem.key.find_last_of("/"));
      }
      return outPath;
   }
   
   string Bucket::exportItem(const BucketItemData& item, string destinationFolder) const {
      S3ResponseHandler responseHandler = {
         &propertiesCallback,
         &completeCallback
      };

      S3GetObjectHandler getObjectHandler =  {
         responseHandler,
         &dataCallback
      };

      string exportFilePath = createExportFilePath(item, destinationFolder);
      callbackData cb;
      cb.FileHandle = fopen(exportFilePath.c_str(), "wb");
 
      S3_get_object(&_bucketcontext, item.key.c_str(), NULL, 0, 0, NULL, 0 ,&getObjectHandler, &cb);

      fclose(cb.FileHandle);
      cb.FileHandle=nullptr;
      return exportFilePath;
   }
}






