(ns oncurrent.zeno.ddb-raw-storage
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn <read-k* [ddb table-name k]
  (au/go
    (let [arg {:op :GetItem
               :request {:TableName table-name
                         :Key {"k" {:S k}}
                         :AttributesToGet ["v"]}}
          ret (au/<? (aws-async/invoke ddb arg))]
      (some-> ret :Item :v :B slurp ba/b64->byte-array))))

(defn <write-k!* [ddb table-name k ba]
  (au/go
    (let [b64 (ba/byte-array->b64 ba)
          arg {:op :PutItem
               :request {:TableName table-name
                         :Item {"k" {:S k}
                                "v" {:B b64}}}}
          ret (au/<? (aws-async/invoke ddb arg))]
      (or (= {} ret)
          (throw (ex-info "<write-k! failed."
                          (u/sym-map arg ret)))))))

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
          (au/<? (<write-k!* ddb table-name k new-ba))

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

  (<read-k [this k]
    (au/go
      (-> (au/<? ddb-promise-chan)
          (<read-k* table-name k)
          (au/<?))))

  (<write-k! [this k ba]
    (au/go
      (-> (au/<? ddb-promise-chan)
          (<write-k!* table-name k ba)
          (au/<?)))))

(defn <active-table? [ddb table-name]
  (au/go
    (let [ret (au/<? (aws-async/invoke ddb {:op :DescribeTable
                                            :request {:TableName table-name}}))]
      (= "ACTIVE" (some->> ret :Table :TableStatus)))))

(defn make-ddb-raw-storage [table-name]
  (let [ddb-promise-chan (ca/promise-chan)]
    (ca/go
      (try
        (let [ddb (aws/client {:api :dynamodb})]
          (ca/>! ddb-promise-chan
                 (if (au/<? (<active-table? ddb table-name))
                   ddb
                   (ex-info
                    "DynamoDB table `" table-name
                    "` does not exist or is not yet active."
                    (u/sym-map table-name)))))
        (catch Exception e
          (ca/>! ddb-promise-chan e))))
    (->DDBRawStorage table-name ddb-promise-chan)))
