(ns unit.crdt-process-cmds-test
  (:require
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt :as crdt]
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
(deftest test-single-value-cmds
  (let [schema l/string-schema
        *next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        cmds [{:arg "ABC"
               :op :set
               :path []}
              {:arg "XYZ"
               :op :set
               :path []}]
        {:keys [ops crdt]} (crdt/process-cmds (u/sym-map cmds make-id schema))
        expected-ops #{{:add-id "I1"
                        :op-type :add-value
                        :path []
                        :value "ABC"}
                       {:add-id "I1"
                        :op-type :delete-value
                        :path []}
                       {:add-id "I2"
                        :op-type :add-value
                        :path []
                        :value "XYZ"}}
        _ (is (= expected-ops ops))
        path []
        v (crdt/get-value (u/sym-map crdt make-id path schema))]
    (is (= "XYZ" v))))

#_
(deftest test-map-cmds
  (let [schema (l/map-schema l/string-schema)
        *next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        cmds [{:arg {"Hello" "There"
                     "Yo" "Friend"}
               :op :set
               :path []}
              {:arg "World"
               :op :set
               :path ["Hello"]}]
        {:keys [ops crdt]} (crdt/process-cmds (u/sym-map cmds make-id schema))
        expected-ops #{{:add-id "I1"
                        :op-type :add-value
                        :path ["Hello"]
                        :value "There"}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["Yo"]
                        :value "Friend"}
                       {:add-id "I1"
                        :op-type :delete-value
                        :path ["Hello"]}
                       {:add-id "I3"
                        :op-type :add-value
                        :path ["Hello"]
                        :value "World"}}
        _ (is (= expected-ops ops))
        path []
        v (crdt/get-value (u/sym-map crdt make-id path schema))
        expected-v {"Hello" "World"
                    "Yo" "Friend"}]
    (is (= expected-v v))))

#_
(deftest test-map-remove
  (let [schema (l/map-schema l/string-schema)
        *next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        cmds [{:arg {"Hello" "There"
                     "Yo" "Friend"}
               :op :set
               :path []}
              {:op :remove
               :path ["Hello"]}]
        {:keys [ops crdt]} (crdt/process-cmds (u/sym-map cmds make-id schema))
        expected-ops #{{:add-id "I1"
                        :op-type :add-value
                        :path ["Hello"]
                        :value "There"}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["Yo"]
                        :value "Friend"}
                       {:add-id "I1"
                        :op-type :delete-value
                        :path ["Hello"]}}
        _ (is (= expected-ops ops))
        path []
        v (crdt/get-value (u/sym-map crdt make-id path schema))
        expected-v {"Yo" "Friend"}]
    (is (= expected-v v))))

#_
(deftest test-record-cmds
  (let [schema the-rec-schema
        *next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        cmds [{:arg {:foo/a 12
                     :bar/b "Hello"
                     :c false}
               :op :set
               :path []}
              {:arg "Goodbye"
               :op :set
               :path [:bar/b]}]
        {:keys [ops crdt]} (crdt/process-cmds (u/sym-map cmds make-id schema))
        expected-ops #{{:add-id "I1"
                        :op-type :add-value
                        :path [:foo/a 1]
                        :value 12}
                       {:add-id "I2"
                        :op-type :add-value
                        :path [:bar/b 1]
                        :value "Hello"}
                       {:add-id "I3"
                        :op-type :add-value
                        :path [:c 1]
                        :value false}
                       {:add-id "I2"
                        :op-type :delete-value
                        :path [:bar/b 1]}
                       {:add-id "I4"
                        :op-type :add-value
                        :path [:bar/b 1]
                        :value "Goodbye"}}
        _ (is (= expected-ops ops))
        path []
        v (crdt/get-value (u/sym-map crdt make-id path schema))
        expected-v {:foo/a 12
                    :bar/b "Goodbye"
                    :c false}]
    (is (= expected-v v))))
#_
(deftest test-record-remove
  (let [schema the-rec-schema
        *next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        cmds [{:arg {:foo/a 12
                     :bar/b "Hello"
                     :c false}
               :op :set
               :path []}
              {:op :remove
               :path [:c]}]
        {:keys [ops crdt]} (crdt/process-cmds (u/sym-map cmds make-id schema))
        expected-ops #{{:add-id "I1"
                        :op-type :add-value
                        :path [:foo/a 1]
                        :value 12}
                       {:add-id "I2"
                        :op-type :add-value
                        :path [:bar/b 1]
                        :value "Hello"}
                       {:add-id "I3"
                        :op-type :add-value
                        :path [:c 1]
                        :value false}
                       {:add-id "I3"
                        :op-type :delete-value
                        :path [:c 1]}}
        _ (is (= expected-ops ops))
        path []
        v (crdt/get-value (u/sym-map crdt make-id path schema))
        expected-v {:foo/a 12
                    :bar/b "Hello"}]
    (is (= expected-v v))))
#_
(deftest test-array-cmds
  (let [schema (l/array-schema pet-schema)
        *next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        cmds [{:arg {:name "Fido"
                     :species "Canis familiaris"}
               :op :insert-after
               :path [0]}
              {:arg {:name "Sleepy"
                     :species "Felis catus"}
               :op :insert-after
               :path [0]}
              {:arg "Bitey"
               :op :set
               :path [1 :name]}
              {:op :remove
               :path [0]}]
        {:keys [ops crdt]} (crdt/process-cmds (u/sym-map cmds make-id schema))
        expected-ops #{{:add-id :id
                        :op-type :add-value
                        :path [:id :name]
                        :value "Fido"}
                       {:add-id :id
                        :op-type :add-value
                        :path [:id :species]
                        :value "Canis familiaris"}
                       {:add-id :id
                        :op-type :add-value
                        :path [:id :name]
                        :value "Sleepy"}
                       {:add-id :id
                        :op-type :add-value
                        :path [:id :species]
                        :value "Felis catus"}}
        _ (is (= expected-ops ops))
        path []
        v (crdt/get-value (u/sym-map crdt make-id path schema))
        expected-v [{:name "Bitey"
                     :species "Canis familiaris"}]]
    (is (= expected-v v))))
