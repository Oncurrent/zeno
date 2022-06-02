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
   [lambdaisland.uri :as uri]
   [ring.adapter.jetty :as jetty]
   [taoensso.timbre :as log])
  (:import
   (com.amazonaws HttpMethod)
   (com.amazonaws.services.s3 AmazonS3Client AmazonS3ClientBuilder)
   (java.io BufferedInputStream)
   (java.util Date)
   (org.eclipse.jetty.server Server)))

(set! *warn-on-reflection* true)

(defprotocol IBulkStorage
  (<delete! [this k])
  (<get [this k])
  (<get-time-limited-url [this k seconds-valid])
  (<put! [this k ba] [this k ba opts])
  (<stop-server! [this]))

(defn <create-s3-bucket! [{:keys [bucket-name location-constraint]}]
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
                                                        (or location-constraint
                                                            "us-west-2")}}}
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
    (or (sr/get cache [bucket-name k])
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
        (do
          (sr/put! cache [bucket-name k] ba)
          true)))))

(defn <s3-get-time-limited-url
  [{:keys [bucket-name java-client k seconds-valid]}]
  (au/go
    (let [expiration-ms (+ (u/current-time-ms) (* 1000 seconds-valid))
          expiration-date (Date. (long expiration-ms))]
      (str (.generatePresignedUrl ^AmazonS3Client java-client
                                  bucket-name
                                  k
                                  expiration-date
                                  (HttpMethod/valueOf "GET"))))))

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
        (do
          (sr/evict! cache [bucket-name k])
          true)))))

(defrecord S3BulkStorage [bucket-name cache java-client s3-client]
  IBulkStorage
  (<delete! [this k]
    (<s3-delete! (u/sym-map bucket-name cache k s3-client)))
  (<get [this k]
    (<s3-get (u/sym-map bucket-name cache k s3-client)))
  (<get-time-limited-url [this k seconds-valid]
    (<s3-get-time-limited-url
     (u/sym-map bucket-name java-client k seconds-valid)))
  (<put! [this k ba]
    (<put! this k ba {}))
  (<put! [this k ba opts]
    (<s3-put! (u/sym-map ba bucket-name cache k opts s3-client)))
  (<stop-server! [this]
    (au/go
      nil)))

(defn <mem-put! [{:keys [*store k ba opts]}]
  (au/go
    (swap! *store assoc k (u/sym-map ba opts))
    true))

(defn <mem-get-time-limited-url [{:keys [host k seconds-valid server-port]}]
  (au/go
    (if-not server-port
      (throw (ex-info (str "Can't get URL because `:server-port` was not "
                           "provided to the ->mem-bulk-storage constructor")
                      {}))
      (let [expiration-ms (+ (u/current-time-ms) (* 1000 seconds-valid))
            query-str (u/map->query-string {:ks [:k :expiration-ms]
                                            :m (u/sym-map k expiration-ms)})]
        (-> (uri/uri "")
            (assoc :scheme "http"
                   :host host
                   :port server-port
                   :query query-str)
            (str))))))

(defn <mem-stop-server! [{:keys [server]}]
  (au/go
    (.stop ^Server server)
    true))

(defrecord MemBulkStorage [*store host server server-port]
  IBulkStorage
  (<delete! [this k]
    (au/go
      (swap! *store dissoc k)
      true))
  (<get [this k]
    (au/go
      (:ba (@*store k))))
  (<get-time-limited-url [this k seconds-valid]
    (<mem-get-time-limited-url (u/sym-map host k seconds-valid server-port)))
  (<put! [this k ba]
    (<put! this k ba {}))
  (<put! [this k ba opts]
    (<mem-put! (u/sym-map *store k ba opts)))
  (<stop-server! [this]
    (<mem-stop-server! (u/sym-map server))))

(defrecord PrefixedBulkStorage [bulk-storage prefix]
  IBulkStorage
  (<delete! [this k]
    (<delete! bulk-storage (str prefix "-" k)))
  (<get [this k]
    (<get bulk-storage (str prefix "-" k)))
  (<get-time-limited-url [this k seconds-valid]
    (<get-time-limited-url this (str prefix "-" k) seconds-valid))
  (<put! [this k ba]
    (<put! bulk-storage (str prefix "-" k) ba))
  (<put! [this k ba opts]
    (<put! bulk-storage (str prefix "-" k) ba opts))
  (<stop-server! [this]
    (<stop-server! bulk-storage)))

(defn opts->headers [opts]
  (let [k->header-k {:cache-control "Cache-Control"
                     :content-disposition "Content-Disposition"
                     :content-encoding "Content-Encoding"
                     :content-type "Content-Type"}]
    (reduce-kv (fn [acc k v]
                 (assoc acc (k->header-k k) v))
               {}
               opts)))

(defn make-server-handler [{:keys [*store]}]
  (fn [{:keys [request-method query-string]}]
    (if (not= :get request-method)
      {:status 405}
      (let [{:keys [k expiration-ms]} (u/query-string->map query-string)
            expired? (when expiration-ms
                       (>= (u/current-time-ms)
                           (u/str->long expiration-ms)))
            {:keys [opts ba]} (@*store k)
            headers (opts->headers opts)]
        (cond
          expired?
          {:status 403
           :headers {"Content-Type" "text/plain"}
           :body "Expired"}

          ba
          {:status 200
           :headers headers
           :body ba}

          :else
          {:status 404
           :headers {"Content-Type" "text/plain"}
           :body "Not found"})))))

(defn ->mem-bulk-storage
  ([]
   (->mem-bulk-storage {}))
  ([{:keys [server-port] :as config}]
   (let [*store (atom {})
         host (or (:host config) "localhost")
         server-handler (make-server-handler (u/sym-map *store))
         server (when server-port
                  (when-not (int? server-port)
                    (throw (ex-info (str "`:server-port` must be an integer. "
                                         "Got `" server-port "`.")
                                    config)))
                  (jetty/run-jetty server-handler {:host host
                                                   :join? false
                                                   :port server-port}))]
     (->MemBulkStorage *store host server server-port))))

(defn ->s3-bulk-storage [{:keys [bucket-name num-cache-slots]
                          :as arg}]
  (when (str/blank? bucket-name)
    (throw (ex-info "You must specify a `:bucket-name`." arg)))
  (let [s3-client (aws/client {:api :s3})
        java-client (-> (doto (AmazonS3ClientBuilder/standard)
                          (.enablePathStyleAccess))
                        (.build))
        cache (sr/stockroom (or num-cache-slots 10))]
    (aws/validate-requests s3-client true)
    (->S3BulkStorage bucket-name cache java-client s3-client)))

(defn ->prefixed-bulk-storage [{:keys [bulk-storage prefix]}]
  (->PrefixedBulkStorage bulk-storage prefix))
