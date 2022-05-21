(ns com.oncurrent.zeno.bulk-storage
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (java.io BufferedInputStream)))

(set! *warn-on-reflection* true)


(defprotocol IBulkStorage
  (<delete! [this k])
  (<get [this k])
  (<put! [this k ba] [this k ba opts]))

(defn <create-s3-bucket! [{:keys [bucket-name]}]
  (au/go
    (when (str/blank? bucket-name)
      (throw (ex-info (str "`bucket-name` argument must be a non-empty string. "
                           "Got: `" bucket-name "`.")
                      (u/sym-map bucket-name))))
    (let [s3-client (aws/client {:api :s3})
          _ (aws/validate-requests s3-client true)
          s3-arg {:op :CreateBucket
                  :request {:Bucket bucket-name
                            :CreateBucketConfiguration {:LocationConstraint
                                                        "us-west-2"}}}
          ret (au/<? (aws-async/invoke s3-client s3-arg))]
      (if (:cognitect.anomalies/category ret)
        (throw (ex-info (str ret)
                        (u/sym-map s3-arg bucket-name ret)))
        true))))

(defn <delete-s3-bucket! [{:keys [bucket-name]}]
  (au/go
    (when (str/blank? bucket-name)
      (throw (ex-info (str "`bucket-name` argument must be a non-empty string. "
                           "Got: `" bucket-name "`.")
                      (u/sym-map bucket-name))))
    (let [s3-client (aws/client {:api :s3})
          _ (aws/validate-requests s3-client true)
          s3-arg {:op :DeleteBucket
                  :request {:Bucket bucket-name}}
          ret (au/<? (aws-async/invoke s3-client s3-arg))]
      (if (:cognitect.anomalies/category ret)
        (throw (ex-info (str ret)
                        (u/sym-map s3-arg bucket-name ret)))
        true))))

(defn <s3-get [{:keys [bucket-name cache s3-client k]}]
  (au/go
    (when-not (string? k)
      (throw (ex-info (str "`k` argument to `<get` must be a string. Got `"
                           k "`")
                      (u/sym-map k))))
    (or (sr/get cache k)
        (let [s3-arg {:op :GetObject
                      :request {:Bucket bucket-name
                                :Key k}}
              ret (au/<? (aws-async/invoke s3-client s3-arg))]
          (case (:cognitect.anomalies/category ret)
            nil
            (let [ba (ba/byte-array (:ContentLength ret))]
              (.read ^BufferedInputStream (:Body ret) ba)
              ba)

            :cognitect.anomalies/not-found
            nil

            ;; default case
            (throw (ex-info (str ret)
                            (u/sym-map s3-arg bucket-name k ret))))))))

(defn <s3-put! [{:keys [ba bucket-name cache k opts s3-client]}]
  (au/go
    (when-not (string? k)
      (throw (ex-info (str "`k` argument to `<put!` must be a string. Got `"
                           k "`")
                      (u/sym-map k))))
    (when-not ba
      (throw (ex-info "`ba` argument to `<put!` must not be nil."
                      (u/sym-map k))))
    (let [{:keys [acl
                  cache-control
                  content-disposition
                  content-encoding
                  content-type]} opts
          req (cond-> {:Body ba
                       :Bucket bucket-name
                       :ContentType (or content-type "application/octet-stream")
                       :Key k}
                acl (assoc :ACL acl)
                cache-control (assoc :CacheControl cache-control)
                content-disposition (assoc :ContentDisposition
                                           content-disposition)
                content-encoding (assoc :ContentEncoding content-encoding))
          s3-arg {:op :PutObject
                  :request req}
          ret (au/<? (aws-async/invoke s3-client s3-arg))]
      (if (:cognitect.anomalies/category ret)
        (throw (ex-info (str ret)
                        (u/sym-map s3-arg bucket-name k ret)))
        true))))

(defn <s3-delete! [{:keys [bucket-name cache k s3-client]}]
  (au/go
    (when-not (string? k)
      (throw (ex-info (str "`k` argument to `<delete!` must be a string. Got `"
                           k "`")
                      (u/sym-map k))))
    (let [s3-arg {:op :DeleteObject
                  :request {:Bucket bucket-name
                            :Key k}}
          ret (au/<? (aws-async/invoke s3-client s3-arg))]
      (if (:cognitect.anomalies/category ret)
        (throw (ex-info (str ret)
                        (u/sym-map s3-arg bucket-name k ret)))
        true))))

(defrecord S3BulkStorage [bucket-name cache s3-client]
  IBulkStorage
  (<delete! [this k]
    (<s3-delete! (u/sym-map bucket-name cache k s3-client)))
  (<get [this k]
    (<s3-get (u/sym-map bucket-name cache k s3-client)))
  (<put! [this k ba]
    (<put! this k ba {}))
  (<put! [this k ba opts]
    (<s3-put! (u/sym-map ba bucket-name cache k opts s3-client))))

(defrecord MemBulkStorage [*store]
  IBulkStorage
  (<delete! [this k]
    (au/go
      (swap! *store dissoc k)
      true))
  (<get [this k]
    (au/go
      (@*store k)))
  (<put! [this k ba]
    (<put! this k ba {}))
  (<put! [this k ba opts]
    (au/go
      (swap! *store assoc k ba)
      true)))

(defn make-mem-bulk-storage []
  (let [*store (atom {})]
    (->MemBulkStorage *store)))

(defn make-s3-bulk-storage [{:keys [bucket-name num-cache-slots] :as arg}]
  (when (str/blank? bucket-name)
    (throw (ex-info "You must specify a `:bucket-name`." arg)))
  (let [s3-client (aws/client {:api :s3})
        cache (sr/stockroom (or num-cache-slots 10))]
    (aws/validate-requests s3-client true)
    (->S3BulkStorage bucket-name cache s3-client)))
