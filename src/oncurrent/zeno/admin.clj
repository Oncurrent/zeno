(ns oncurrent.zeno.admin
  (:require
   [clojure.core.async :as ca]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [deercreeklabs.async-utils :as au]
   [oncurrent.zeno.ddb-raw-storage :as drs]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

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
           (or (au/<? (drs/<active-table? ddb table-name))
               (do
                 (ca/<! (ca/timeout create-attempt-delay-ms))
                 (recur (dec attempts-left))))))))))
