(ns oncurrent.zeno.admin
  (:require
   [clojure.core.async :as ca]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [deercreeklabs.async-utils :as au]
   [oncurrent.zeno.ddb-raw-storage :as drs]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def default-create-branch-options
  {:authenticators []
   :source-tx-id nil
   :use-temp-storage false})

(defn <create-branch
  ([storage branch-name]
   (<create-branch storage branch-name {}))
  ([storage branch-name options]
   (au/go
     (let [branch-info (merge default-create-branch-options options)]
       (<)))))

(defn <list-branches [storage]
  (au/go
    ))

(defn <get-branch-info [storage branch-name]
  (au/go
    ))

(defn <add-authenticator [storage branch-name authenticator-name]
  (au/go
    ))

(defn <remove-authenticator [storage branch-name authenticator-name]
  (au/go
    ))
