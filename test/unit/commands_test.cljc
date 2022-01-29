(ns unit.commands-test
  (:require
   [clojure.set :as set]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.commands :as commands]
   [oncurrent.zeno.crdt :as crdt]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(l/def-record-schema pet-schema
  [:name l/string-schema]
  [:species l/string-schema])

(l/def-record-schema pet-owner-schema
  [:name l/string-schema]
  [:pets (l/array-schema pet-schema)])

(deftest test-crdt-set
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:arg "Hi"
                     :op :set
                     :path [:crdt]}]
             :crdt-store-schema l/string-schema
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I1"
                        :op-type :add-value
                        :path []
                        :sys-time-ms sys-time-ms
                        :value "Hi"}}
        expected-value "Hi"]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-set-and-remove
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:arg "Hello"
                     :op :set
                     :path [:crdt]}
                    {:op :remove
                     :path [:crdt]}]
             :crdt-store-schema l/string-schema
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I1"
                        :op-type :add-value
                        :path []
                        :sys-time-ms sys-time-ms
                        :value "Hello"}
                       {:add-id "I1"
                        :op-type :delete-value
                        :path []}}
        expected-value nil]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-set-and-reset
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:arg "Hello"
                     :op :set
                     :path [:crdt]}
                    {:arg "Goodbye"
                     :op :set
                     :path [:crdt]}]
             :crdt-store-schema l/string-schema
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I1"
                        :op-type :add-value
                        :path []
                        :sys-time-ms sys-time-ms
                        :value "Hello"}
                       {:add-id "I1"
                        :op-type :delete-value
                        :path []}
                       {:add-id "I2"
                        :op-type :add-value
                        :path []
                        :sys-time-ms sys-time-ms
                        :value "Goodbye"}}
        expected-value "Goodbye"]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-map-set-and-reset
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:arg 31
                     :op :set
                     :path [:crdt "Alice"]}
                    {:arg 8
                     :op :set
                     :path [:crdt "Bob"]}
                    {:arg 12
                     :op :set
                     :path [:crdt "Bob"]}]
             :crdt-store-schema (l/map-schema l/int-schema)
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I2"
                        :op-type :delete-value
                        :path ["Bob"]}
                       {:add-id "I1"
                        :op-type :add-value
                        :path ["Alice"]
                        :sys-time-ms sys-time-ms
                        :value 31}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["Bob"]
                        :sys-time-ms sys-time-ms
                        :value 8}
                       {:add-id "I3"
                        :op-type :add-value
                        :path ["Bob"]
                        :sys-time-ms sys-time-ms
                        :value 12}}
        expected-value {"Alice" 31
                        "Bob" 12}]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-record-set-and-reset
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:arg "Lamby"
                     :op :set
                     :path [:crdt :name]}
                    {:arg "Ovis aries"
                     :op :set
                     :path [:crdt :species]}
                    {:arg "Sheepy"
                     :op :set
                     :path [:crdt :name]}]
             :crdt-store-schema pet-schema
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I1"
                        :op-type :delete-value
                        :path [:name 1]}
                       {:add-id "I1"
                        :op-type :add-value
                        :path [:name 1]
                        :sys-time-ms sys-time-ms
                        :value "Lamby"}
                       {:add-id "I2"
                        :op-type :add-value
                        :path [:species 1]
                        :sys-time-ms sys-time-ms
                        :value "Ovis aries"}
                       {:add-id "I3"
                        :op-type :add-value,
                        :path [:name 1]
                        :sys-time-ms sys-time-ms
                        :value "Sheepy"}}
        expected-value {:name "Sheepy"
                        :species "Ovis aries"}]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-union-set-and-reset
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:arg 3.14
                     :op :set
                     :path [:crdt]}
                    {:arg "pi"
                     :op :set
                     :path [:crdt]}]
             :crdt-store-schema (l/union-schema [l/string-schema l/float-schema])
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I1"
                        :op-type :delete-value
                        :path [1]}
                       {:add-id "I1"
                        :op-type :add-value
                        :path [1]
                        :sys-time-ms sys-time-ms
                        :value 3.14}
                       {:add-id "I2"
                        :op-type :add-value
                        :path [0]
                        :sys-time-ms sys-time-ms
                        :value "pi"}}
        expected-value "pi"]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest  test-crdt-array-set
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:arg ["Hi" "There"]
                     :op :set
                     :path [:crdt]}]
             :crdt-store-schema (l/array-schema l/string-schema)
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I5"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I3"
                                :tail-node-id "-END-"}}
                       {:add-id "I6"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "-START-"
                                :tail-node-id "I1"}}
                       {:add-id "I7"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I1"
                                :tail-node-id "I3"}}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["I1"]
                        :sys-time-ms sys-time-ms
                        :value "Hi"}
                       {:add-id "I4"
                        :op-type :add-value
                        :path ["I3"]
                        :sys-time-ms sys-time-ms
                        :value "There"}}
        expected-value ["Hi" "There"]]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-set-and-remove
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:arg ["Hi" "There"]
                     :op :set
                     :path [:crdt]}
                    {:op :remove
                     :path [:crdt 0]}]
             :crdt-store-schema (l/array-schema l/string-schema)
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I6"
                        :op-type :delete-array-edge
                        :path []}
                       {:add-id "I7"
                        :op-type :delete-array-edge
                        :path []}
                       {:add-id "I2"
                        :op-type :delete-value
                        :path ["I1"]}
                       {:add-id "I5"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I3"
                                :tail-node-id "-END-"}}
                       {:add-id "I6"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "-START-"
                                :tail-node-id "I1"}}
                       {:add-id "I7"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I1"
                                :tail-node-id "I3"}}
                       {:add-id "I8"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "-START-"
                                :tail-node-id "I3"}}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["I1"]
                        :sys-time-ms sys-time-ms
                        :value "Hi"}
                       {:add-id "I4"
                        :op-type :add-value
                        :path ["I3"]
                        :sys-time-ms sys-time-ms
                        :value "There"}}
        expected-value ["There"]]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-set-and-reset
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:arg ["Hi" "There"]
                     :op :set
                     :path [:crdt]}
                    {:arg "Go"
                     :op :set
                     :path [:crdt 0]}]
             :crdt-store-schema (l/array-schema l/string-schema)
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I2"
                        :op-type :delete-value
                        :path ["I1"]}
                       {:add-id "I5"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I3"
                                :tail-node-id "-END-"}}
                       {:add-id "I6"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "-START-"
                                :tail-node-id "I1"}}
                       {:add-id "I7"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I1"
                                :tail-node-id "I3"}}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["I1"]
                        :sys-time-ms sys-time-ms
                        :value "Hi"}
                       {:add-id "I4"
                        :op-type :add-value
                        :path ["I3"]
                        :sys-time-ms sys-time-ms
                        :value "There"}
                       {:add-id "I8"
                        :op-type :add-value
                        :path ["I1"]
                        :sys-time-ms sys-time-ms
                        :value "Go"}}
        expected-value ["Go" "There"]]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-simple-inserts
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:arg ["Hi" "There"]
                     :op :set
                     :path [:crdt]}
                    {:arg "Hello!"
                     :op :insert-before
                     :path [:crdt 0]}
                    {:arg "Bob"
                     :op :insert-after
                     :path [:crdt -1]}]
             :crdt-store-schema (l/array-schema l/string-schema)
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I5"
                        :op-type :delete-array-edge
                        :path []}
                       {:add-id "I6"
                        :op-type :delete-array-edge
                        :path []}
                       {:add-id "I11"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "-START-"
                                :tail-node-id "I8"}}
                       {:add-id "I10"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms,
                        :value {:head-node-id "I8"
                                :tail-node-id "I1"}}
                       {:add-id "I14"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I3"
                                :tail-node-id "I12"}}
                       {:add-id "I15"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I12"
                                :tail-node-id "-END-"}}
                       {:add-id "I5"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I3"
                                :tail-node-id "-END-"}}
                       {:add-id "I6"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "-START-"
                                :tail-node-id "I1"}}
                       {:add-id "I7"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I1"
                                :tail-node-id "I3"}}
                       {:add-id "I13"
                        :op-type :add-value
                        :path ["I12"]
                        :sys-time-ms sys-time-ms
                        :value "Bob"}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["I1"]
                        :sys-time-ms sys-time-ms
                        :value "Hi"}
                       {:add-id "I4"
                        :op-type :add-value
                        :path ["I3"]
                        :sys-time-ms sys-time-ms
                        :value "There"}
                       {:add-id "I9"
                        :op-type :add-value
                        :path ["I8"]
                        :sys-time-ms sys-time-ms
                        :value "Hello!"}}
        expected-value ["Hello!" "Hi" "There" "Bob"]]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-insert-before-into-empty
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:arg "Hello!"
                     :op :insert-before
                     :path [:crdt 0]}]
             :crdt-store-schema (l/array-schema l/string-schema)
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I4"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "-START-"
                                :tail-node-id "I1"}}
                       {:add-id "I3"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I1"
                                :tail-node-id "-END-"}}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["I1"]
                        :sys-time-ms sys-time-ms
                        :value "Hello!"}}
        expected-value ["Hello!"]]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-insert-after-into-empty
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:arg "Hello!"
                     :op :insert-after
                     :path [:crdt -1]}]
             :crdt-store-schema (l/array-schema l/string-schema)
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I3"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "-START-"
                                :tail-node-id "I1"}}
                       {:add-id "I4"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I1"
                                :tail-node-id "-END-"}}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["I1"]
                        :sys-time-ms sys-time-ms
                        :value "Hello!"}}
        expected-value ["Hello!"]]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-insert-range-after-into-empty
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:arg ["1" "2" "3"]
                     :op :insert-range-after
                     :path [:crdt -1]}]
             :crdt-store-schema (l/array-schema l/string-schema)
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I10"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I3"
                                :tail-node-id "I5"}}
                       {:add-id "I7"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I5"
                                :tail-node-id "-END-"}}
                       {:add-id "I8"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "-START-"
                                :tail-node-id "I1"}}
                       {:add-id "I9"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I1"
                                :tail-node-id "I3"}}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["I1"]
                        :sys-time-ms sys-time-ms
                        :value "1"}
                       {:add-id "I4"
                        :op-type :add-value
                        :path ["I3"]
                        :sys-time-ms sys-time-ms
                        :value "2"}
                       {:add-id "I6"
                        :op-type :add-value
                        :path ["I5"]
                        :sys-time-ms sys-time-ms
                        :value "3"}}
        expected-value ["1" "2" "3"]]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-insert-range-before-into-empty
  (let [*next-id-num (atom 0)
        sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:arg ["1" "2" "3"]
                     :op :insert-range-before
                     :path [:crdt 0]}]
             :crdt-store-schema (l/array-schema l/string-schema)
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))
             :sys-time-ms sys-time-ms}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-ops #{{:add-id "I10"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I3"
                                :tail-node-id "I5"}}
                       {:add-id "I7"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I5"
                                :tail-node-id "-END-"}}
                       {:add-id "I8"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "-START-"
                                :tail-node-id "I1"}}
                       {:add-id "I9"
                        :op-type :add-array-edge
                        :path []
                        :sys-time-ms sys-time-ms
                        :value {:head-node-id "I1"
                                :tail-node-id "I3"}}
                       {:add-id "I2"
                        :op-type :add-value
                        :path ["I1"]
                        :sys-time-ms sys-time-ms
                        :value "1"}
                       {:add-id "I4"
                        :op-type :add-value
                        :path ["I3"]
                        :sys-time-ms sys-time-ms
                        :value "2"}
                       {:add-id "I6"
                        :op-type :add-value
                        :path ["I5"]
                        :sys-time-ms sys-time-ms
                        :value "3"}}
        expected-value ["1" "2" "3"]]
    (is (= expected-ops ops))
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-insert-range-before-into-front
  (let [arg {:cmds [{:arg ["4" "5"]
                     :op :set
                     :path [:crdt]}
                    {:arg ["1" "2" "3"]
                     :op :insert-range-before
                     :path [:crdt 0]}]
             :crdt-store-schema (l/array-schema l/string-schema)}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-value ["1" "2" "3" "4" "5"]]
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-insert-range-after-end
  (let [arg {:cmds [{:arg ["4" "5"]
                     :op :set
                     :path [:crdt]}
                    {:arg ["1" "2" "3"]
                     :op :insert-range-after
                     :path [:crdt -1]}]
             :crdt-store-schema (l/array-schema l/string-schema)}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-value ["4" "5" "1" "2" "3"]]
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-insert-range-before-into-middle
  (let [arg {:cmds [{:arg ["1" "2" "3"]
                     :op :set
                     :path [:crdt]}
                    {:arg ["A" "B"]
                     :op :insert-range-before
                     :path [:crdt -2]}]
             :crdt-store-schema (l/array-schema l/string-schema)}
        {:keys [crdt-store ops]} (commands/process-cmds arg)
        expected-value ["1" "A" "B" "2" "3"]]
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-crdt-array-insert-range-after-into-middle
  (let [arg {:cmds [{:arg ["1" "2" "3"]
                     :op :set
                     :path [:crdt]}
                    {:arg ["A" "B"]
                     :op :insert-range-after
                     :path [:crdt -2]}]
             :crdt-store-schema (l/array-schema l/string-schema)}
        {:keys [crdt-store]} (commands/process-cmds arg)
        expected-value ["1" "2" "A" "B" "3"]]
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-nested-set
  (let [arg {:cmds [{:arg {:name "Bill"
                           :pets [{:name "Pinky"
                                   :species "Felis catus"}
                                  {:name "Fishy"
                                   :species "Carassius auratus"}]}
                     :op :set
                     :path [:crdt]}
                    {:arg "Goldy"
                     :op :set
                     :path [:crdt :pets -1 :name]}]
             :crdt-store-schema pet-owner-schema}
        {:keys [crdt-store]} (commands/process-cmds arg)
        expected-value {:name "Bill"
                        :pets [{:name "Pinky"
                                :species "Felis catus"}
                               {:name "Goldy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))
    (is (= {:name "Pinky"
            :species "Felis catus"}
           (crdt/get-value {:crdt crdt-store
                            :path [:pets -2]
                            :schema (:crdt-store-schema arg)})))))

(deftest test-nested-set-and-remove
  (let [arg {:cmds [{:arg {:name "Bill"
                           :pets [{:name "Pinky"
                                   :species "Felis catus"}
                                  {:name "Fishy"
                                   :species "Carassius auratus"}]}
                     :op :set
                     :path [:crdt]}
                    {:op :remove
                     :path [:crdt :pets 0]}]
             :crdt-store-schema pet-owner-schema}
        {:keys [crdt-store]} (commands/process-cmds arg)
        expected-value {:name "Bill"
                        :pets [{:name "Fishy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value (crdt/get-value {:crdt crdt-store
                                           :path []
                                           :schema (:crdt-store-schema arg)})))))

(deftest test-subsequent-txns
  (let [arg1 {:cmds [{:arg {:name "Bill"
                            :pets [{:name "Pinky"
                                    :species "Felis catus"}
                                   {:name "Fishy"
                                    :species "Carassius auratus"}]}
                      :op :set
                      :path [:crdt]}]
              :crdt-store-schema pet-owner-schema}
        ret1 (commands/process-cmds arg1)
        arg2 {:cmds [{:op :remove
                      :path [:crdt :pets 0]}]
              :crdt-store (:crdt-store ret1)
              :crdt-store-schema pet-owner-schema}
        ret2 (commands/process-cmds arg2)
        expected-value {:name "Bill"
                        :pets [{:name "Fishy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value (crdt/get-value {:crdt (:crdt-store ret2)
                                           :path []
                                           :schema (:crdt-store-schema arg1)})))))

(deftest test-nested-merge-array-no-conflict
  (let [arg0 {:cmds [{:arg {:name "Bill"
                            :pets [{:name "Pinky"
                                    :species "Felis catus"}
                                   {:name "Fishy"
                                    :species "Carassius auratus"}]}
                      :op :set
                      :path [:crdt]}]
              :crdt-store-schema pet-owner-schema}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:arg {:name "Chris"
                            :species "Canis familiaris"}
                      :op :insert-before
                      :path [:crdt :pets 0]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema pet-owner-schema}
        arg2 {:cmds [{:arg {:name "Pat"
                            :species "Canis familiaris"}
                      :op :insert-after
                      :path [:crdt :pets -1]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema pet-owner-schema}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        new-ops (set/union (:ops ret1) (:ops ret2))
        merged-crdt (crdt/apply-ops {:crdt (:crdt-store ret0)
                                     :ops new-ops
                                     :schema pet-owner-schema})
        expected-value [{:name "Chris"
                         :species "Canis familiaris"}
                        {:name "Pinky"
                         :species "Felis catus"}
                        {:name "Fishy"
                         :species "Carassius auratus"}
                        {:name "Pat"
                         :species "Canis familiaris"}]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path [:pets]
                                           :schema pet-owner-schema})))))

(deftest test-nested-merge-array-conflict
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        arg0 {:cmds [{:arg {:name "Bill"
                            :pets [{:name "Pinky"
                                    :species "Felis catus"}
                                   {:name "Fishy"
                                    :species "Carassius auratus"}]}
                      :op :set
                      :path [:crdt]}]
              :crdt-store-schema pet-owner-schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:arg {:name "Chris"
                            :species "Canis familiaris"}
                      :op :insert-before
                      :path [:crdt :pets 0]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema pet-owner-schema
              :make-id make-id}
        arg2 {:cmds [{:arg {:name "Pat"
                            :species "Canis familiaris"}
                      :op :insert-before
                      :path [:crdt :pets 0]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema pet-owner-schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        new-ops (set/union (:ops ret1) (:ops ret2))
        merged-crdt (crdt/apply-ops {:crdt (:crdt-store ret0)
                                     :ops new-ops
                                     :schema pet-owner-schema})
        expected-value [{:name "Chris"
                         :species "Canis familiaris"}
                        {:name "Pat"
                         :species "Canis familiaris"}
                        {:name "Pinky"
                         :species "Felis catus"}
                        {:name "Fishy"
                         :species "Carassius auratus"}]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path [:pets]
                                           :schema pet-owner-schema})))))

(deftest test-merge-3-way-array-conflict
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        arg0 {:cmds [{:arg ["A" "B"]
                      :op :set
                      :path [:crdt]}]
              :crdt-store-schema schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:arg "1"
                      :op :insert-before
                      :path [:crdt 0]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema schema
              :make-id make-id}
        arg2 {:cmds [{:arg "2"
                      :op :insert-before
                      :path [:crdt 0]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema schema
              :make-id make-id}
        arg3 {:cmds [{:arg "3"
                      :op :insert-before
                      :path [:crdt 0]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        ret3 (commands/process-cmds arg3)
        new-ops (set/union (:ops ret1) (:ops ret2) (:ops ret3))
        merged-crdt (crdt/apply-ops {:crdt (:crdt-store ret0)
                                     :ops new-ops
                                     :schema schema})
        ;; Order of the first three items is determined by their add-ids.
        ;; Any ordering would be fine, as long as it is deterministic.
        ;; The important part is that all three appear before the original
        ;; sequence (["A" "B"]).
        expected-value ["2" "3" "1" "A" "B"]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path []
                                           :schema schema})))))

(deftest test-merge-3-way-array-conflict-multiple-peer-cmds
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        arg0 {:cmds [{:arg ["A" "B"]
                      :op :set
                      :path [:crdt]}]
              :crdt-store-schema schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:arg "X1"
                      :op :insert-before
                      :path [:crdt 0]}
                     {:arg "X2"
                      :op :insert-after
                      :path [:crdt 0]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema schema
              :make-id make-id}
        arg2 {:cmds [{:arg "Y1"
                      :op :insert-before
                      :path [:crdt 0]}
                     {:arg "Y2"
                      :op :insert-after
                      :path [:crdt 0]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema schema
              :make-id make-id}
        arg3 {:cmds [{:arg "Z1"
                      :op :insert-before
                      :path [:crdt 0]}
                     {:arg "Z2"
                      :op :insert-after
                      :path [:crdt 0]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        ret3 (commands/process-cmds arg3)
        new-ops (set/union (:ops ret1) (:ops ret2) (:ops ret3))
        merged-crdt (crdt/apply-ops {:crdt (:crdt-store ret0)
                                     :ops new-ops
                                     :schema schema})
        ;; Order of the first three pairs of items is determined by their
        ;; add-ids. Any ordering would be fine, as long as it is deterministic.
        ;; The important part is that all three appear before the original
        ;; sequence (["A" "B"]) and that the Xs, Ys, and Zs are not interleaved.
        expected-value ["Y1" "Y2" "Z1" "Z2" "X1" "X2" "A" "B"]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path []
                                           :schema schema})))))

(deftest test-merge-multiple-conflicts
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        arg0 {:cmds [{:arg ["A" "B" "C"]
                      :op :set
                      :path [:crdt]}]
              :crdt-store-schema schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:arg "XF"
                      :op :insert-before
                      :path [:crdt 0]}
                     {:arg "XL"
                      :op :insert-after
                      :path [:crdt -1]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema schema
              :make-id make-id}
        arg2 {:cmds [{:arg "YF"
                      :op :insert-before
                      :path [:crdt 0]}
                     {:arg "YL"
                      :op :insert-after
                      :path [:crdt -1]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema schema
              :make-id make-id}
        arg3 {:cmds [{:arg "ZF"
                      :op :insert-before
                      :path [:crdt 0]}
                     {:arg "ZL"
                      :op :insert-after
                      :path [:crdt -1]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        ret3 (commands/process-cmds arg3)
        new-ops (set/union (:ops ret1) (:ops ret2) (:ops ret3))
        merged-crdt (crdt/apply-ops {:crdt (:crdt-store ret0)
                                     :ops new-ops
                                     :schema schema})
        expected-value ["XF" "YF" "ZF" "A" "B" "C" "XL" "YL" "ZL"]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :make-id make-id
                                           :path []
                                           :schema schema})))))

(deftest test-nested-merge-conflict
  (let [arg0 {:cmds [{:arg {:name "Bill"
                            :pets [{:name "Pinky"
                                    :species "Felis catus"}
                                   {:name "Fishy"
                                    :species "Carassius auratus"}]}
                      :op :set
                      :path [:crdt]}]
              :crdt-store-schema pet-owner-schema}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:arg "Goldy"
                      :op :set
                      :path [:crdt :pets -1 :name]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema pet-owner-schema
              :sys-time-ms (u/str->long "1640205282000")}
        arg2 {:cmds [{:arg "Herman"
                      :op :set
                      :path [:crdt :pets -1 :name]}]
              :crdt-store (:crdt-store ret0)
              :crdt-store-schema pet-owner-schema
              :sys-time-ms (u/str->long "1640205282999")}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        merged-crdt (crdt/apply-ops {:crdt (:crdt-store ret0)
                                     :ops (set/union (:ops ret1) (:ops ret2))
                                     :schema pet-owner-schema})]
    ;; We expect "Herman" because its sys-time-ms is later
    (is (= "Herman" (crdt/get-value {:crdt merged-crdt
                                     :path [:pets -1 :name]
                                     :schema pet-owner-schema})))))

(deftest test-set-into-empty-array
  (let [arg {:cmds [{:arg "1"
                     :op :set
                     :path [:crdt]}]
             :crdt-store-schema (l/array-schema l/string-schema)}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"arg is not sequential"
         (commands/process-cmds arg)))))
