(ns com.oncurrent.zeno.server.aws
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def s3-client (aws/client {:api :s3}))

(defn <read-k* [ddb table-name k]
  (au/go
    (let [arg {:op :GetItem
               :request {:TableName table-name
                         :Key {"k" {:S k}}
                         :AttributesToGet ["v"]}}
          ret (au/<? (aws-async/invoke ddb arg))]
      (some-> ret :Item :v :B slurp ba/b64->byte-array))))

(defn <add-k!* [ddb table-name k ba]
  (au/go
    (let [b64 (ba/byte-array->b64 ba)
          arg {:op :PutItem
               :request {:ConditionExpression "attribute_not_exists(k)"
                         :TableName table-name
                         :Item {"k" {:S k}
                                "v" {:B b64}}}}
          ret (au/<? (aws-async/invoke ddb arg))]
      (if-not (= :cognitect.anomalies/incorrect
                 (:cognitect.anomalies/category ret))
        (= {} ret)
        (if (str/includes? (:__type ret) "ConditionalCheckFailedException")
          (throw (ex-info (str "Key `" k "` already exists.")
                          (u/sym-map k)))
          (throw (ex-info (str "Unknown error in <add-k!: " (:__type ret))
                          ret)))))))

(defn <put-k!* [ddb table-name k ba]
  (au/go
    (let [b64 (ba/byte-array->b64 ba)
          arg {:op :PutItem
               :request {:TableName table-name
                         :Item {"k" {:S k}
                                "v" {:B b64}}}}
          ret (au/<? (aws-async/invoke ddb arg))]
      (if-not (= :cognitect.anomalies/incorrect
                 (:cognitect.anomalies/category ret))
        (= {} ret)
        (throw (ex-info (str "Unknown error in <put-k!: " (:__type ret))
                        ret))))))

(defn <delete-k!* [ddb table-name k]
  (au/go
    (let [arg {:op :DeleteItem
               :request {:TableName table-name
                         :Key {"k" {:S k}}}}
          ret (au/<? (aws-async/invoke ddb arg))]
      (or (= {} ret)
          (throw (ex-info "<delete-k! failed."
                          (u/sym-map arg ret)))))))

(defn <compare-and-set-k!* [ddb table-name k old-ba new-ba]
  (au/go
    (let [old-b64 (ba/byte-array->b64 old-ba)
          new-b64 (ba/byte-array->b64 new-ba)
          arg {:op :UpdateItem
               :request {:TableName table-name
                         :Key {"k" {:S k}}
                         :ExpressionAttributeValues {":oldv"
                                                     (if old-b64
                                                       {:B old-b64}
                                                       {:NULL true})
                                                     ":newv" {:B new-b64}}
                         :UpdateExpression "SET v = :newv"
                         :ConditionExpression
                         "attribute_not_exists(v) OR v = :oldv"
                         :ReturnValues "UPDATED_NEW"}}
          ret  (au/<? (aws-async/invoke ddb arg))]
      (if-not (= :cognitect.anomalies/incorrect
                 (:cognitect.anomalies/category ret))
        (= new-b64 (some-> ret :Attributes :v :B slurp))
        (cond
          (str/includes? (:__type ret) "ResourceNotFoundException")
          (au/<? (<add-k!* ddb table-name k new-ba))

          (str/includes? (:__type ret) "ConditionalCheckFailedException")
          false

          :else
          (throw (ex-info (str "Unknown error in UpdateItem: " (:__type ret))
                          ret)))))))

(defrecord DDBRawStorage [table-name ddb-promise-chan]
  storage/IRawStorage
  (<compare-and-set-k! [this k old-ba new-ba]
    (au/go
      (-> (au/<? ddb-promise-chan)
          (<compare-and-set-k!* table-name k old-ba new-ba)
          (au/<?))))

  (<delete-k! [this k]
    (au/go
      (-> (au/<? ddb-promise-chan)
          (<delete-k!* table-name k)
          (au/<?))))

  (get-max-value-bytes [this]
    350000)

  (<read-k [this k]
    (au/go
      (-> (au/<? ddb-promise-chan)
          (<read-k* table-name k)
          (au/<?))))

  (<add-k! [this k ba]
    (au/go
      (-> (au/<? ddb-promise-chan)
          (<add-k!* table-name k ba)
          (au/<?))))

  (<put-k! [this k ba]
    (au/go
      (-> (au/<? ddb-promise-chan)
          (<put-k!* table-name k ba)
          (au/<?)))))

(defn <active-table? [ddb table-name]
  (au/go
    (let [ret (au/<? (aws-async/invoke ddb {:op :DescribeTable
                                            :request {:TableName table-name}}))]
      (= "ACTIVE" (some->> ret :Table :TableStatus)))))

(def default-ddb-table-options
  {:billing-mode "PAY_PER_REQUEST"
   :create-attempt-delay-ms 1000
   :max-create-attempts 10})

(defn <create-ddb-table
  ([table-name]
   (<create-ddb-table table-name {}))
  ([table-name options]
   (au/go
     (let [ddb (aws/client {:api :dynamodb})
           options* (merge default-ddb-table-options options)
           {:keys [create-attempt-delay-ms
                   billing-mode
                   max-create-attempts]} options*]
       (aws-async/invoke ddb
                         {:op :CreateTable
                          :request {:TableName table-name
                                    :KeySchema [{:AttributeName "k"
                                                 :KeyType "HASH"}]
                                    :AttributeDefinitions [{:AttributeName "k"
                                                            :AttributeType "S"}]
                                    :BillingMode billing-mode}})
       (loop [attempts-left max-create-attempts]
         (if (zero? attempts-left)
           (throw
            (ex-info (str "Could not create DynamoDB table `" table-name "`.")
                     {:table-name table-name}))
           (or (au/<? (<active-table? ddb table-name))
               (do
                 (ca/<! (ca/timeout create-attempt-delay-ms))
                 (recur (dec attempts-left))))))))))

(defn make-ddb-raw-storage [table-name]
  (let [ddb-promise-chan (ca/promise-chan)]
    (ca/go
      (try
        (let [ddb (aws/client {:api :dynamodb})]
          (ca/>! ddb-promise-chan
                 (if (au/<? (<active-table? ddb table-name))
                   ddb
                   (ex-info
                    (str "DynamoDB table `" table-name
                         "` does not exist or is not yet active.")
                    (u/sym-map table-name)))))
        (catch Exception e
          (ca/>! ddb-promise-chan e))))
    (->DDBRawStorage table-name ddb-promise-chan)))


(defn <s3-publish-member-urls [bucket k urls]
  (au/go
    (let [body (-> (str/join "\n" urls)
                   (ba/utf8->byte-array))
          s3-arg {:op :PutObject
                  :request {:ACL "public-read"
                            :Body body
                            :Bucket bucket
                            :CacheControl "no-cache"
                            :ContentType "text/plain"
                            :Key k}}
          ret (au/<? (aws-async/invoke s3-client s3-arg))]
      (if (:cognitect.anomalies/category ret)
        (throw (ex-info (str ret)
                        (u/sym-map s3-arg ret)))
        true))))

(defn <s3-get-published-member-urls [bucket k]
  (au/go
    (let [s3-arg {:op :GetObject
                  :request {:Bucket bucket
                            :Key k}}
          ret (au/<? (aws-async/invoke s3-client s3-arg))]
      (if (:cognitect.anomalies/category ret)
        (throw (ex-info (str ret)
                        (u/sym-map s3-arg ret)))
        (let [ba (ba/byte-array (:ContentLength ret))]
          (.read (:Body ret) ba)
          (->> (ba/byte-array->utf8 ba)
               (str/split-lines)))))))
