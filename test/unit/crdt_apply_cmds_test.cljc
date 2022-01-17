(ns unit.crdt-apply-cmds-test
  (:require
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt :as crdt]
   [oncurrent.zeno.crdt.apply-cmds :as ac]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(l/def-record-schema the-rec-schema
  [:foo/a l/int-schema]
  [:bar/b l/string-schema]
  [:c l/boolean-schema])

(l/def-record-schema pet-schema
  [:name l/string-schema]
  [:species l/string-schema])

(l/def-record-schema pet-owner-schema
  [:name l/string-schema]
  [:pets (l/array-schema pet-schema)])

#_
(deftest ^:this test-single-value-xl
  (let [schema l/string-schema
        *next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "a" n))
        cmds [{:arg "ABC"
               :op :set
               :path []}
              {:arg "XYZ"
               :op :set
               :path []}]
        {:keys [ops]} (ac/apply-cmds {:cmds cmds
                                      :crdt nil
                                      :make-id make-id
                                      :schema schema})
        expected-ops #{{:add-id "a1"
                        :op-type :add-value
                        :path []
                        :value "ABC"}
                       {:add-id "a1"
                        :op-type :delete-value
                        :path []}
                       {:add-id "a2"
                        :op-type :add-value
                        :path []
                        :value "XYZ"}}]
    (is (= expected-ops ops))))