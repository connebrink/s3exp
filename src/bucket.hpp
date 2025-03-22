 
#include <vector>
#include <string>
#include <functional>

#include <libs3.h>

namespace s3exp {
   using namespace std;
   class Bucket
   {

   public:
      struct BucketItemData {
         string key;
         string etag;
         unsigned long long size;
         BucketItemData()  : size(0) {}
      };

      struct BucketItemsList {
         vector<BucketItemData> items;
         uint64_t count;
         uint64_t size;
         BucketItemsList() : count(0), size(0) {}
      };

   private:
      S3BucketContext _bucketcontext;

   private:
      struct callbackData {
         Bucket *bucket = nullptr;
         Bucket::BucketItemsList *items = nullptr;
         S3Status s3Status;
         S3BucketContext bucketContext;
         function<void(const Bucket::BucketItemData&)> action = nullptr;
         int isTruncated = 0;
         string nextMarker;
         FILE *FileHandle = nullptr;
         int maxKeyCount = 0;
         bool maxKeyCountIsReached = false;
      };

   private:
      string createExportFilePath(const Bucket::BucketItemData& bucketItem, string& exportPath) const;
      static void completeCallback(S3Status status, const S3ErrorDetails *error, void *cbData);
      static S3Status dataCallback(int bufferSize, const char *buffer, void *callbackData);
      static S3Status propertiesCallback(const S3ResponseProperties *properties, void *callbackData);
      static S3Status listCallback(int isTruncated, const char *nextMarker, int contentsCount,
                                   const S3ListBucketContent *contents, int commonPrefixesCount,
                                   const char **commonPrefixes, void *cbData);
      
   public:
      Bucket(const S3BucketContext& bucketContext) : _bucketcontext(bucketContext) {
      }

   public:
      BucketItemsList getItems(int maxKeyCount=0, function<void(const BucketItemData&)> action=nullptr) const;
      string exportItem(const BucketItemData& item, string destinationFolder) const;
   };
}
