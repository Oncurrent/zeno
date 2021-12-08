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
   :initial-branch-log-file nil
   :source-tx-id nil
   :use-temp-storage false})

(defn check-options [options]
  ;; TODO
  )

(defn <create-branch
  ([storage branch-name]
   (<create-branch storage branch-name {}))
  ([storage branch-name options]
   (au/go
     (let [options* (merge default-create-branch-options options)
           _ (check-options options*)]
       ))))

(defn <list-branches [storage]
  (au/go
    ))

(defn <get-branch-info [storage branch-name]
  (au/go
    ))

(defn <export-branch-log [storage branch-name output-file]
  (au/go
    ))

(defn <add-authenticator [storage branch-name authenticator-name]
  (au/go
    ))

(defn <remove-authenticator [storage branch-name authenticator-name]
  (au/go
    ))
